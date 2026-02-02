#![doc = include_str!("../README.md")]

mod error;
mod row;
mod serialize;
// mod transaction_builder;
// mod transaction_manager;

use core::future::Future;

use std::collections::{HashMap, HashSet};

use diesel::{
    ConnectionError,
    deserialize::FromSqlRow,
    pg::{
        Pg, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgQueryBuilder, PgTypeMetadata,
    },
    query_builder::{
        AsQuery, QueryBuilder, QueryFragment, QueryId, bind_collector::RawBytesBindCollector,
    },
    query_dsl::CompatibleType,
    result::{Error, QueryResult},
};
use tokio::sync::Mutex as AsyncMutex;
use xitca_postgres::{
    Execute, RowStreamOwned,
    iter::AsyncLendingIterator,
    pool::{CachedStatement, Pool, PoolConnection},
    statement::{Statement, StatementNamed},
    types::Type,
};

use self::{row::PgRow, serialize::ToSqlHelper};

// pub use transaction_builder::TransactionBuilder;

const FAKE_OID: u32 = 0;

pub struct AsyncPgConnection {
    pool: Pool,
    meta: Meta,
}

type Meta = AsyncMutex<PgMetadataCache>;

impl AsyncPgConnection {
    pub async fn establish(url: &str) -> Result<Self, ConnectionError> {
        let pool = Pool::builder(url)
            .build()
            .map_err(error::into_connection_error)?;

        Ok(Self {
            pool,
            meta: Default::default(),
        })
    }

    pub async fn transaction<F, T>(&self, exec: F) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut TransactionConnection<'_>) -> QueryResult<T>,
    {
        let conn = self.pool.get().await.map_err(error::into_error)?;
        let mut conn = TransactionConnection {
            conn,
            meta: &self.meta,
        };
        exec(&mut conn).await
    }
}

/// async version of [`diesel::query_dsl::RunQueryDsl`]
pub trait RunQueryDsl<C>
where
    Self: AsQuery + Send,
    Self::Query: QueryFragment<Pg> + QueryId + Send,
{
    /// async version of [`diesel::query_dsl::RunQueryDsl::execute`]
    fn execute(self, conn: C) -> impl Future<Output = QueryResult<usize>> + Send;

    #[doc(hidden)]
    fn load_stream(self, conn: C) -> impl Future<Output = QueryResult<RowStreamOwned>> + Send;

    /// async version of [`diesel::query_dsl::RunQueryDsl::load`]
    fn load<U>(self, conn: C) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        C: Send,
        Self: Sized,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let mut res = Vec::new();
            self.load_into(conn, &mut res).await.map(|_| res)
        }
    }

    /// alternative version of [`diesel::query_dsl::RunQueryDsl::load_iter`] where the iteration
    /// happens inside the method with given collection type. when function returns sucessfully
    /// the given collection type would be populated with types converted from row data
    fn load_into<U, R>(
        self,
        conn: C,
        collection: &mut R,
    ) -> impl Future<Output = QueryResult<()>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        R: Extend<U> + Send,
        C: Send,
        Self: Sized,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let stream = self.load_stream(conn).await?;
            try_collect_into(stream, collection).await
        }
    }

    /// async version of [`diesel::query_dsl::RunQueryDsl::get_result`]
    fn get_result<U>(self, conn: C) -> impl Future<Output = QueryResult<U>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        C: Send,
        Self: Sized,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let mut stream = self.load_stream(conn).await?;
            try_next(&mut stream).await?.ok_or_else(|| Error::NotFound)
        }
    }

    /// alternative version of [`diesel::query_dsl::RunQueryDsl::get_results`]
    #[inline]
    fn get_results<U>(self, conn: C) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        C: Send,
        Self: Sized,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        self.load(conn)
    }

    /// async version of [`diesel::query_dsl::RunQueryDsl::first`]
    #[inline]
    fn first<U>(self, conn: C) -> impl Future<Output = QueryResult<U>> + Send
    where
        U: FromSqlRow<
                <<diesel::dsl::Limit<Self> as AsQuery>::SqlType as CompatibleType<U, Pg>>::SqlType,
                Pg,
            > + Send,
        C: Send,
        Self: diesel::query_dsl::methods::LimitDsl + Sized,
        diesel::dsl::Limit<Self>: RunQueryDsl<C>,
        <diesel::dsl::Limit<Self> as AsQuery>::Query: QueryFragment<Pg> + QueryId + Send,
        <diesel::dsl::Limit<Self> as AsQuery>::SqlType: CompatibleType<U, Pg>,
    {
        diesel::query_dsl::methods::LimitDsl::limit(self, 1).get_result(conn)
    }
}

impl<Q> RunQueryDsl<&AsyncPgConnection> for Q
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    async fn execute(self, conn: &AsyncPgConnection) -> QueryResult<usize> {
        let res = {
            let mut pool_conn = conn.pool.get().await.map_err(error::into_error)?;
            execute(self, &mut pool_conn, &conn.meta).await?
        }
        .await
        .map_err(error::into_error)?;

        Ok(res as _)
    }

    async fn load_stream(self, conn: &AsyncPgConnection) -> QueryResult<RowStreamOwned> {
        let mut pool_conn = conn.pool.get().await.map_err(error::into_error)?;
        load(self, &mut pool_conn, &conn.meta).await
    }
}

pub struct TransactionConnection<'a> {
    conn: PoolConnection<'a>,
    meta: &'a Meta,
}

impl<Q> RunQueryDsl<&mut TransactionConnection<'_>> for Q
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    async fn execute(self, conn: &mut TransactionConnection<'_>) -> QueryResult<usize> {
        let res = execute(self, &mut conn.conn, conn.meta)
            .await?
            .await
            .map_err(error::into_error)?;
        Ok(res as _)
    }

    async fn load_stream(
        self,
        conn: &mut TransactionConnection<'_>,
    ) -> QueryResult<RowStreamOwned> {
        load(self, &mut conn.conn, conn.meta).await
    }
}

async fn execute<Q>(
    query: Q,
    conn: &mut PoolConnection<'_>,
    meta: &Meta,
) -> QueryResult<impl Future<Output = Result<u64, xitca_postgres::Error>> + 'static>
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    execute_with(query, conn, meta, async |conn, stmt, bind_collector| {
        let binds = bind_collector
            .metadata
            .into_iter()
            .zip(bind_collector.binds)
            .map(ToSqlHelper);

        Ok(stmt.bind(binds).execute(&*conn))
    })
    .await
}

async fn load<Q>(
    query: Q,
    conn: &mut PoolConnection<'_>,
    meta: &Meta,
) -> QueryResult<RowStreamOwned>
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    execute_with(query, conn, meta, async |conn, stmt, bind_collector| {
        let binds = bind_collector
            .metadata
            .into_iter()
            .zip(bind_collector.binds)
            .map(ToSqlHelper);

        stmt.bind(binds)
            .into_owned()
            .query(&*conn)
            .await
            .map_err(error::into_error)
    })
    .await
}

async fn execute_with<Q, E, R>(
    query: Q,
    conn: &mut PoolConnection<'_>,
    meta: &Meta,
    exec: E,
) -> QueryResult<R>
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
    E: AsyncFnOnce(
        &mut PoolConnection<'_>,
        CachedStatement,
        RawBytesBindCollector<Pg>,
    ) -> QueryResult<R>,
{
    let query = query.as_query();

    let mut query_builder = PgQueryBuilder::default();

    let bind_data = construct_bind_data(&query)?;

    query.to_sql(&mut query_builder, &Pg)?;

    let sql = query_builder.finish();

    let BindData {
        fake_oid_locations,
        generated_oids,
        mut bind_collector,
    } = bind_data;

    if let Some(ref unresolved_types) = generated_oids {
        let metadata_cache = &mut *meta.lock().await;
        let mut real_oids = HashMap::new();

        for ((schema, lookup_type_name), (fake_oid, fake_array_oid)) in unresolved_types {
            // for each unresolved item
            // we check whether it's already in the cache
            // or perform a lookup and insert it into the cache
            let cache_key =
                PgMetadataCacheKey::new(schema.as_deref().map(Into::into), lookup_type_name.into());
            let real_metadata = if let Some(type_metadata) = metadata_cache.lookup_type(&cache_key)
            {
                type_metadata
            } else {
                let type_metadata = lookup_type(schema, lookup_type_name, conn).await?;
                metadata_cache.store_type(cache_key, type_metadata);

                PgTypeMetadata::from_result(Ok(type_metadata))
            };
            // let (fake_oid, fake_array_oid) = metadata_lookup.fake_oids(index);
            let (real_oid, real_array_oid) = unwrap_oids(&real_metadata);
            real_oids.extend([(*fake_oid, real_oid), (*fake_array_oid, real_array_oid)]);
        }

        // Replace fake OIDs with real OIDs in `bind_collector.metadata`
        for m in &mut bind_collector.metadata {
            let (oid, array_oid) = unwrap_oids(m);
            *m = PgTypeMetadata::new(
                real_oids.get(&oid).copied().unwrap_or(oid),
                real_oids.get(&array_oid).copied().unwrap_or(array_oid),
            );
        }

        // Replace fake OIDs with real OIDs in `bind_collector.binds`
        for (bind_index, byte_index) in fake_oid_locations {
            replace_fake_oid(
                &mut bind_collector.binds,
                &real_oids,
                bind_index,
                byte_index,
            )?;
        }
    }

    let bind_types = bind_collector
        .metadata
        .iter()
        .map(type_from_oid)
        .collect::<QueryResult<Vec<_>>>()?;

    let stmt = xitca_postgres::Statement::named(&sql, &bind_types)
        .execute(&mut *conn)
        .await
        .map_err(error::into_error)?;

    exec(conn, stmt, bind_collector).await
}

async fn try_collect_into<U, St, C>(
    mut stream: RowStreamOwned,
    collection: &mut C,
) -> QueryResult<()>
where
    U: FromSqlRow<St, Pg> + Send,
    C: Extend<U>,
{
    while let Some(item) = try_next(&mut stream).await? {
        collection.extend(Some(item));
    }
    Ok(())
}

async fn try_next<U, St>(stream: &mut RowStreamOwned) -> QueryResult<Option<U>>
where
    U: FromSqlRow<St, Pg> + Send,
{
    match stream.try_next().await {
        Ok(Some(row)) => {
            let item = U::build_from_row(&PgRow::new(row)).map_err(Error::DeserializationError)?;
            Ok(Some(item))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(error::into_error(e)),
    }
}

async fn lookup_type(
    schema: &Option<String>,
    type_name: &String,
    conn: &mut PoolConnection<'_>,
) -> QueryResult<(u32, u32)> {
    match *schema {
        Some(ref schema) => LOOK_UP.bind([type_name, schema]).query(conn),
        None => LOOK_UP_NO_SCHEMA.bind([type_name]).query(conn),
    }
    .await
    .map_err(error::into_error)?
    .try_next()
    .await
    .map_err(error::into_error)?
    .ok_or_else(|| Error::NotFound)
    .map(|r| (r.get(0), r.get(1)))
}

fn type_from_oid(t: &PgTypeMetadata) -> QueryResult<Type> {
    let oid = t
        .oid()
        .map_err(|e| Error::SerializationError(Box::new(e) as _))?;

    Ok(Type::from_oid(oid).unwrap_or_else(|| {
        Type::new(
            format!("diesel_custom_type_{oid}"),
            oid,
            xitca_postgres::types::Kind::Simple,
            "public".into(),
        )
    }))
}

struct BindData {
    fake_oid_locations: Vec<(usize, usize)>,
    generated_oids: GeneratedOidTypeMap,
    bind_collector: RawBytesBindCollector<Pg>,
}

fn construct_bind_data(query: &dyn QueryFragment<Pg>) -> Result<BindData, Error> {
    // we don't resolve custom types here yet, we do that later
    // in the async block below as we might need to perform lookup
    // queries for that.
    //
    // We apply this workaround to prevent requiring all the diesel
    // serialization code to being async
    //
    // We give out constant fake oids here to optimize for the "happy" path
    // without custom type lookup
    let mut bind_collector_0 = RawBytesBindCollector::<Pg>::new();
    let mut metadata_lookup_0 = PgAsyncMetadataLookup {
        custom_oid: false,
        generated_oids: None,
        oid_generator: |_, _| (FAKE_OID, FAKE_OID),
    };
    let collect_bind_result_0 =
        query.collect_binds(&mut bind_collector_0, &mut metadata_lookup_0, &Pg);
    // we have encountered a custom type oid, so we need to perform more work here.
    // These oids can occur in two locations:
    //
    // * In the collected metadata -> relatively easy to resolve, just need to replace them below
    // * As part of the serialized bind blob -> hard to replace
    //
    // To address the second case, we perform a second run of the bind collector
    // with a different set of fake oids. Then we compare the output of the two runs
    // and use that information to infer where to replace bytes in the serialized output
    if metadata_lookup_0.custom_oid {
        // we try to get the maxium oid we encountered here
        // to be sure that we don't accidently give out a fake oid below that collides with
        // something
        let mut max_oid = bind_collector_0
            .metadata
            .iter()
            .flat_map(|t| {
                [
                    t.oid().unwrap_or_default(),
                    t.array_oid().unwrap_or_default(),
                ]
            })
            .max()
            .unwrap_or_default();
        let mut bind_collector_1 = RawBytesBindCollector::<diesel::pg::Pg>::new();
        let mut metadata_lookup_1 = PgAsyncMetadataLookup {
            custom_oid: false,
            generated_oids: Some(HashMap::new()),
            oid_generator: move |_, _| {
                max_oid += 2;
                (max_oid, max_oid + 1)
            },
        };
        let collect_bind_result_1 =
            query.collect_binds(&mut bind_collector_1, &mut metadata_lookup_1, &Pg);

        assert_eq!(
            bind_collector_0.binds.len(),
            bind_collector_0.metadata.len()
        );

        let fake_oid_locations = std::iter::zip(
            bind_collector_0
                .binds
                .iter()
                .zip(&bind_collector_0.metadata),
            &bind_collector_1.binds,
        )
        .enumerate()
        .flat_map(|(bind_index, ((bytes_0, metadata_0), bytes_1))| {
            // custom oids might appear in the serialized bind arguments for arrays or composite (record) types
            // in both cases the relevant buffer is a custom type on it's own
            // so we only need to check the cases that contain a fake OID on their own
            let (bytes_0, bytes_1) = if matches!(metadata_0.oid(), Ok(FAKE_OID)) {
                (
                    bytes_0.as_deref().unwrap_or_default(),
                    bytes_1.as_deref().unwrap_or_default(),
                )
            } else {
                // for all other cases, just return an empty
                // list to make the iteration below a no-op
                // and prevent the need of boxing
                (&[] as &[_], &[] as &[_])
            };
            let lookup_map = metadata_lookup_1
                .generated_oids
                .as_ref()
                .map(|map| {
                    map.values()
                        .flat_map(|(oid, array_oid)| [*oid, *array_oid])
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();
            std::iter::zip(
                bytes_0.windows(std::mem::size_of_val(&FAKE_OID)),
                bytes_1.windows(std::mem::size_of_val(&FAKE_OID)),
            )
            .enumerate()
            .filter_map(move |(byte_index, (l, r))| {
                // here we infer if some byte sequence is a fake oid
                // We use the following conditions for that:
                //
                // * The first byte sequence matches the constant FAKE_OID
                // * The second sequence does not match the constant FAKE_OID
                // * The second sequence is contained in the set of generated oid,
                //   otherwise we get false positives around the boundary
                //   of a to be replaced byte sequence
                let r_val = u32::from_be_bytes(r.try_into().expect("That's the right size"));
                (l == FAKE_OID.to_be_bytes()
                    && r != FAKE_OID.to_be_bytes()
                    && lookup_map.contains(&r_val))
                .then_some((bind_index, byte_index))
            })
        })
        // Avoid storing the bind collectors in the returned Future
        .collect();

        collect_bind_result_0
            .and(collect_bind_result_1)
            .map(|_| BindData {
                fake_oid_locations,
                generated_oids: metadata_lookup_1.generated_oids,
                bind_collector: bind_collector_1,
            })
    } else {
        collect_bind_result_0.map(|_| BindData {
            fake_oid_locations: Vec::new(),
            generated_oids: None,
            bind_collector: bind_collector_0,
        })
    }
}

type GeneratedOidTypeMap = Option<HashMap<(Option<String>, String), (u32, u32)>>;

// Collects types that need to be looked up, and causes fake OIDs to be written into the bind collector
// so they can be replaced with asynchronously fetched OIDs after the original query is dropped
struct PgAsyncMetadataLookup<F: FnMut(&str, Option<&str>) -> (u32, u32) + 'static> {
    custom_oid: bool,
    generated_oids: GeneratedOidTypeMap,
    oid_generator: F,
}

impl<F> PgMetadataLookup for PgAsyncMetadataLookup<F>
where
    F: FnMut(&str, Option<&str>) -> (u32, u32) + 'static,
{
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        self.custom_oid = true;

        let oid = if let Some(ref mut map) = self.generated_oids {
            *map.entry((schema.map(ToOwned::to_owned), type_name.to_owned()))
                .or_insert_with(|| (self.oid_generator)(type_name, schema))
        } else {
            (self.oid_generator)(type_name, schema)
        };

        PgTypeMetadata::from_result(Ok(oid))
    }
}

const LOOK_UP: StatementNamed<'_> = Statement::named(
    "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
    INNER JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid \
    WHERE pg_type.typname = $1 AND pg_namespace.nspname = $2 \
    LIMIT 1",
    &[],
);

const LOOK_UP_NO_SCHEMA: StatementNamed<'_> = Statement::named(
    "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
    WHERE pg_type.oid = quote_ident($1)::regtype::oid \
    LIMIT 1",
    &[],
);

fn unwrap_oids(metadata: &PgTypeMetadata) -> (u32, u32) {
    let err_msg = "PgTypeMetadata is supposed to always be Ok here";
    (
        metadata.oid().expect(err_msg),
        metadata.array_oid().expect(err_msg),
    )
}

fn replace_fake_oid(
    binds: &mut [Option<Vec<u8>>],
    real_oids: &HashMap<u32, u32>,
    bind_index: usize,
    byte_index: usize,
) -> QueryResult<()> {
    binds
        .get_mut(bind_index)
        .and_then(|bytes| bytes.as_mut())
        .and_then(|bytes| bytes.get_mut(byte_index..))
        .and_then(|bytes| bytes.first_chunk_mut::<4>())
        .and_then(|serialized_oid| {
            real_oids
                .get(&u32::from_be_bytes(*serialized_oid))
                .map(|oid| *serialized_oid = oid.to_be_bytes())
        })
        .ok_or_else(|| {
            Error::SerializationError(
                format!(
                "diesel_async failed to replace a type OID serialized in bind value {bind_index}"
            )
                .into(),
            )
        })
}
