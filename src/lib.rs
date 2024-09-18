//! diesel-async api powered by xitca-postgres db driver

use core::future::{Future, IntoFuture};

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use diesel::{
    connection::{
        statement_cache::StatementCacheKey, Instrumentation, InstrumentationEvent, StrQueryHelper,
    },
    pg::{
        Pg, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgQueryBuilder, PgTypeMetadata,
    },
    query_builder::{
        bind_collector::RawBytesBindCollector, AsQuery, QueryBuilder, QueryFragment, QueryId,
    },
    result::{ConnectionError, ConnectionResult, DatabaseErrorKind, Error, QueryResult},
};
use diesel_async::{pooled_connection::PoolableConnection, AsyncConnection, SimpleAsyncConnection};
use futures_util::{
    future::BoxFuture,
    stream::{BoxStream, StreamExt, TryStreamExt},
};
use tokio::sync::{broadcast, Mutex};
use xitca_postgres::{
    compat::{RowStreamOwned, StatementGuarded},
    types::Type,
    Client,
};

use cache::{PrepareCallback, StmtCache};
use error_helper::ErrorHelper;
use row::PgRow;
use serialize::ToSqlHelper;
use transaction_manager::AnsiTransactionManager;

pub use transaction_builder::TransactionBuilder;

mod cache;
mod error_helper;
mod row;
mod serialize;
mod transaction_builder;
mod transaction_manager;

const FAKE_OID: u32 = 0;

pub(crate) type Statement = StatementGuarded<Arc<Client>>;

/// A connection to a PostgreSQL database.
///
/// Connection URLs should be in the form
/// `postgres://[user[:password]@]host/database_name`
///
/// Checkout the documentation of the [xitca-postgres] crate for details about the format
///
/// [xitca-postgres]: https://github.com/HFQR/xitca-web/postgres
///
/// ```rust
/// # include!("../doctest_setup.rs");
/// use diesel_async::RunQueryDsl;
///
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run_test().await.unwrap();
/// # }
/// #
/// # async fn run_test() -> QueryResult<()> {
/// #     use diesel::sql_types::{Text, Integer};
/// #     let conn = &mut establish_connection().await;
///       let q1 = diesel::select(1_i32.into_sql::<Integer>());
///       let q2 = diesel::select(2_i32.into_sql::<Integer>());
///
///       // construct multiple futures for different queries
///       let f1 = q1.get_result::<i32>(conn);
///       let f2 = q2.get_result::<i32>(conn);
///
///       // wait on both results
///       let res = futures_util::try_join!(f1, f2)?;
///
///       assert_eq!(res.0, 1);
///       assert_eq!(res.1, 2);
///       # Ok(())
/// # }
pub struct AsyncPgConnection {
    conn: Arc<Client>,
    stmt_cache: Arc<Mutex<StmtCache>>,
    transaction_state: Arc<Mutex<AnsiTransactionManager>>,
    metadata_cache: Arc<Mutex<PgMetadataCache>>,
    error_joiner: Option<broadcast::Receiver<Arc<xitca_postgres::Error>>>,
    // a sync mutex is fine here as we only hold it for a really short time
    instrumentation: Arc<std::sync::Mutex<Option<Box<dyn Instrumentation>>>>,
}

impl SimpleAsyncConnection for AsyncPgConnection {
    fn batch_execute<'s, 'q, 'f>(&'s mut self, query: &'q str) -> BoxFuture<'f, QueryResult<()>>
    where
        's: 'f,
        'q: 'f,
    {
        self.record_instrumentation(InstrumentationEvent::start_query(&StrQueryHelper::new(
            query,
        )));
        let error_joiner = self.error_joiner.as_ref().map(|rx| rx.resubscribe());
        let batch_execute = self.conn.execute_simple(query);
        Box::pin(async {
            let res = match batch_execute.await {
                Ok(_) => Ok(()),
                Err(e) => Err(error_joiner.join(e).await),
            };

            self.record_instrumentation(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(query),
                res.as_ref().err(),
            ));

            res
        })
    }
}

impl AsyncConnection for AsyncPgConnection {
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<PgRow>>;
    type Row<'conn, 'query> = PgRow;
    type Backend = Pg;
    type TransactionManager = AnsiTransactionManager;

    fn establish<'d, 'f>(database_url: &'d str) -> BoxFuture<'f, ConnectionResult<Self>>
    where
        'd: 'f,
    {
        let mut instrumentation = diesel::connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::start_establish_connection(
            database_url,
        ));
        let instrumentation = Arc::new(std::sync::Mutex::new(instrumentation));
        Box::pin(async move {
            let (client, driver) = xitca_postgres::Postgres::new(database_url)
                .connect()
                .await
                .map_err(ErrorHelper)?;

            let (tx, rx) = broadcast::channel(1);
            tokio::spawn(async move {
                // TODO: diesel async should be treat driver graceful shutdown as non error.
                let e = driver
                    .into_future()
                    .await
                    .err()
                    .unwrap_or_else(|| xitca_postgres::error::DriverDown.into());
                let _ = tx.send(Arc::new(e));
            });

            let r = Self::setup(client, Some(rx), Arc::clone(&instrumentation)).await;
            instrumentation
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .on_connection_event(InstrumentationEvent::finish_establish_connection(
                    database_url,
                    r.as_ref().err(),
                ));
            r
        })
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        self.with_prepared_statement(source.as_query(), load_prepared)
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        self.with_prepared_statement(source, execute_prepared)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        Arc::get_mut(&mut self.transaction_state)
            .expect("Cannot access shared transaction state")
            .get_mut()
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        Arc::get_mut(&mut self.instrumentation)
            .expect("Cannot access shared instrumentation")
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Arc::new(std::sync::Mutex::new(Some(Box::new(instrumentation))));
    }
}

async fn load_prepared(
    conn: Arc<Client>,
    stmt: Statement,
    binds: Vec<ToSqlHelper>,
) -> Result<BoxStream<'static, Result<PgRow, Error>>, xitca_postgres::Error> {
    let res = conn.query_raw(stmt.as_ref(), binds)?;
    Ok(RowStreamOwned::from(res)
        .map_ok(PgRow::new)
        .map_err(|e| Error::from(ErrorHelper(e)))
        .boxed())
}

async fn execute_prepared(
    conn: Arc<Client>,
    stmt: Statement,
    binds: Vec<ToSqlHelper>,
) -> Result<usize, xitca_postgres::Error> {
    let res = Client::execute_raw(&conn, stmt.as_ref(), binds).await?;
    Ok(res as usize)
}

#[inline(always)]
fn update_transaction_manager_status<T>(
    query_result: QueryResult<T>,
    transaction_manager: &mut AnsiTransactionManager,
) -> QueryResult<T> {
    if let Err(Error::DatabaseError(DatabaseErrorKind::SerializationFailure, _)) = query_result {
        transaction_manager
            .status
            .set_requires_rollback_maybe_up_to_top_level(true)
    }
    query_result
}

impl PrepareCallback for Arc<Client> {
    async fn prepare(&self, sql: &str, metadata: &[PgTypeMetadata]) -> QueryResult<Statement> {
        let bind_types = metadata
            .iter()
            .map(type_from_oid)
            .collect::<QueryResult<Vec<_>>>()?;

        let stmt = Client::prepare(self, sql, &bind_types)
            .await
            .map_err(ErrorHelper)?
            .leak();
        Ok(StatementGuarded::new(stmt, self.clone()))
    }
}

fn type_from_oid(t: &PgTypeMetadata) -> QueryResult<Type> {
    let oid = t
        .oid()
        .map_err(|e| Error::SerializationError(Box::new(e) as _))?;

    if let Some(tpe) = Type::from_oid(oid) {
        return Ok(tpe);
    }

    Ok(Type::new(
        format!("diesel_custom_type_{oid}"),
        oid,
        xitca_postgres::types::Kind::Simple,
        "public".into(),
    ))
}

impl AsyncPgConnection {
    /// Build a transaction, specifying additional details such as isolation level
    ///
    /// See [`TransactionBuilder`] for more examples.
    ///
    /// [`TransactionBuilder`]: crate::pg::TransactionBuilder
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// # use scoped_futures::ScopedFutureExt;
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .read_only()
    ///     .serializable()
    ///     .deferrable()
    ///     .run(|conn| async move { Ok(()) }.scope_boxed())
    ///     .await
    /// # }
    /// ```
    pub fn build_transaction(&mut self) -> TransactionBuilder<Self> {
        TransactionBuilder::new(self)
    }

    /// Construct a new `AsyncPgConnection` instance from an existing [`tokio_postgres::Client`]
    pub async fn try_from(conn: Client) -> ConnectionResult<Self> {
        Self::setup(
            conn,
            None,
            Arc::new(std::sync::Mutex::new(
                diesel::connection::get_default_instrumentation(),
            )),
        )
        .await
    }

    async fn setup(
        conn: Client,
        connection_future: Option<broadcast::Receiver<Arc<xitca_postgres::Error>>>,
        instrumentation: Arc<std::sync::Mutex<Option<Box<dyn Instrumentation>>>>,
    ) -> ConnectionResult<Self> {
        let mut conn = Self {
            conn: Arc::new(conn),
            stmt_cache: Arc::new(Mutex::new(StmtCache::new())),
            transaction_state: Arc::new(Mutex::new(AnsiTransactionManager::default())),
            metadata_cache: Arc::new(Mutex::new(PgMetadataCache::new())),
            error_joiner: connection_future,
            instrumentation,
        };
        conn.set_config_options()
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(conn)
    }

    /// Constructs a cancellation token that can later be used to request cancellation of a query running on the connection associated with this client.
    pub fn cancel_token(&self) -> xitca_postgres::Session {
        self.conn.cancel_token()
    }

    async fn set_config_options(&mut self) -> QueryResult<()> {
        use diesel_async::RunQueryDsl;
        let res1 = diesel::sql_query("SET TIME ZONE 'UTC'").execute(self);
        let res2 = diesel::sql_query("SET CLIENT_ENCODING TO 'UTF8'").execute(self);
        res1.await?;
        res2.await?;
        Ok(())
    }

    fn with_prepared_statement<'a, T, F, R>(
        &mut self,
        query: T,
        callback: fn(Arc<Client>, Statement, Vec<ToSqlHelper>) -> F,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        T: QueryFragment<Pg> + QueryId,
        F: Future<Output = Result<R, xitca_postgres::Error>> + Send + 'a,
        R: Send,
    {
        self.record_instrumentation(InstrumentationEvent::start_query(&diesel::debug_query(
            &query,
        )));
        // we explicilty descruct the query here before going into the async block
        //
        // That's required to remove the send bound from `T` as we have translated
        // the query type to just a string (for the SQL) and a bunch of bytes (for the binds)
        // which both are `Send`.
        // We also collect the query id (essentially an integer) and the safe_to_cache flag here
        // so there is no need to even access the query in the async block below
        let mut query_builder = PgQueryBuilder::default();

        let bind_data = construct_bind_data(&query);

        // The code that doesn't need the `T` generic parameter is in a separate function to reduce LLVM IR lines
        self.with_prepared_statement_after_sql_built(
            callback,
            query.is_safe_to_cache_prepared(&Pg),
            T::query_id(),
            query.to_sql(&mut query_builder, &Pg),
            query_builder,
            bind_data,
        )
    }

    fn with_prepared_statement_after_sql_built<'a, F, R>(
        &mut self,
        callback: fn(Arc<Client>, Statement, Vec<ToSqlHelper>) -> F,
        is_safe_to_cache_prepared: QueryResult<bool>,
        query_id: Option<std::any::TypeId>,
        to_sql_result: QueryResult<()>,
        query_builder: PgQueryBuilder,
        bind_data: BindData,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        F: Future<Output = Result<R, xitca_postgres::Error>> + Send + 'a,
        R: Send,
    {
        let raw_connection = self.conn.clone();
        let stmt_cache = self.stmt_cache.clone();
        let metadata_cache = self.metadata_cache.clone();
        let tm = self.transaction_state.clone();
        let instrumentation = self.instrumentation.clone();
        let BindData {
            collect_bind_result,
            fake_oid_locations,
            generated_oids,
            mut bind_collector,
        } = bind_data;
        let error_joiner = self.error_joiner.as_ref().map(|rx| rx.resubscribe());

        Box::pin(async move {
            let sql = to_sql_result.map(|_| query_builder.finish())?;
            let res = async {
                let is_safe_to_cache_prepared = is_safe_to_cache_prepared?;
                collect_bind_result?;
                // Check whether we need to resolve some types at all
                //
                // If the user doesn't use custom types there is no need
                // to borther with that at all
                if let Some(ref unresolved_types) = generated_oids {
                    let metadata_cache = &mut *metadata_cache.lock().await;
                    let mut real_oids = HashMap::new();

                    for ((schema, lookup_type_name), (fake_oid, fake_array_oid)) in unresolved_types
                    {
                        // for each unresolved item
                        // we check whether it's arleady in the cache
                        // or perform a lookup and insert it into the cache
                        let cache_key = PgMetadataCacheKey::new(
                            schema.as_deref().map(Into::into),
                            lookup_type_name.into(),
                        );
                        let real_metadata =
                            if let Some(type_metadata) = metadata_cache.lookup_type(&cache_key) {
                                type_metadata
                            } else {
                                let type_metadata = lookup_type(
                                    schema.clone(),
                                    lookup_type_name.clone(),
                                    &raw_connection,
                                )
                                .await?;
                                metadata_cache.store_type(cache_key, type_metadata);

                                PgTypeMetadata::from_result(Ok(type_metadata))
                            };
                        // let (fake_oid, fake_array_oid) = metadata_lookup.fake_oids(index);
                        let (real_oid, real_array_oid) = unwrap_oids(&real_metadata);
                        real_oids
                            .extend([(*fake_oid, real_oid), (*fake_array_oid, real_array_oid)]);
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
                        replace_fake_oid(&mut bind_collector.binds, &real_oids, bind_index, byte_index)
                            .ok_or_else(|| {
                                Error::SerializationError(
                                    format!("diesel_async failed to replace a type OID serialized in bind value {bind_index}").into(),
                                )
                            })?;
                    }
                }
                let key = match query_id {
                    Some(id) => StatementCacheKey::Type(id),
                    None => StatementCacheKey::Sql {
                        sql: sql.clone(),
                        bind_types: bind_collector.metadata.clone(),
                    },
                };
                let stmt = stmt_cache
                    .lock()
                    .await
                    .cached_prepared_statement(
                        key,
                        sql.clone(),
                        is_safe_to_cache_prepared,
                        &bind_collector.metadata,
                        &raw_connection,
                        &instrumentation,
                    )
                    .await?;

                let binds = bind_collector
                    .metadata
                    .into_iter()
                    .zip(bind_collector.binds)
                    .map(|(meta, bind)| ToSqlHelper(meta, bind))
                    .collect();
                match callback(raw_connection, stmt, binds).await {
                    Ok(res) => Ok(res),
                    Err(e) => Err(error_joiner.join(e).await),
                }
            };
            let r = update_transaction_manager_status(res.await, &mut *tm.lock().await);
            instrumentation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    r.as_ref().err(),
                ));
            r
        })
    }

    fn record_instrumentation(&self, event: InstrumentationEvent<'_>) {
        self.instrumentation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .on_connection_event(event);
    }
}

/// transform xitca_postgres::Error to diesel::result::Error on certain condition.
trait JoinError: Sized {
    fn join(self, e: xitca_postgres::Error) -> BoxFuture<'static, Error>;
}

impl JoinError for Option<broadcast::Receiver<Arc<xitca_postgres::Error>>> {
    #[cold]
    #[inline(never)]
    fn join(self, e: xitca_postgres::Error) -> BoxFuture<'static, Error> {
        Box::pin(async {
            // when xitca_postgres emit driver shutdown error it means it's Driver
            // task has shutdown already. in this case just await for the driver error
            // to showup and replace client's error type.
            if e.is_driver_down() {
                if let Some(mut rx) = self {
                    let e = rx.recv().await.unwrap();
                    return error_helper::from_tokio_postgres_error(e);
                }
            }
            ErrorHelper(e).into()
        })
    }
}

struct BindData {
    collect_bind_result: Result<(), Error>,
    fake_oid_locations: Vec<(usize, usize)>,
    generated_oids: GeneratedOidTypeMap,
    bind_collector: RawBytesBindCollector<Pg>,
}

fn construct_bind_data(query: &dyn QueryFragment<Pg>) -> BindData {
    // we don't resolve custom types here yet, we do that later
    // in the async block below as we might need to perform lookup
    // queries for that.
    //
    // We apply this workaround to prevent requiring all the diesel
    // serialization code to beeing async
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
    // These oids can occure in two locations:
    //
    // * In the collected metadata -> relativly easy to resolve, just need to replace them below
    // * As part of the seralized bind blob -> hard to replace
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
        .collect::<Vec<_>>();
        BindData {
            collect_bind_result: collect_bind_result_0.and(collect_bind_result_1),
            fake_oid_locations,
            generated_oids: metadata_lookup_1.generated_oids,
            bind_collector: bind_collector_1,
        }
    } else {
        BindData {
            collect_bind_result: collect_bind_result_0,
            fake_oid_locations: Vec::new(),
            generated_oids: None,
            bind_collector: bind_collector_0,
        }
    }
}

type GeneratedOidTypeMap = Option<HashMap<(Option<String>, String), (u32, u32)>>;

/// Collects types that need to be looked up, and causes fake OIDs to be written into the bind collector
/// so they can be replaced with asynchronously fetched OIDs after the original query is dropped
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

        let oid = if let Some(map) = &mut self.generated_oids {
            *map.entry((schema.map(ToOwned::to_owned), type_name.to_owned()))
                .or_insert_with(|| (self.oid_generator)(type_name, schema))
        } else {
            (self.oid_generator)(type_name, schema)
        };

        PgTypeMetadata::from_result(Ok(oid))
    }
}

const LOOK_UP: &str = "\
SELECT pg_type.oid, pg_type.typarray FROM pg_type \
INNER JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid \
WHERE pg_type.typname = $1 AND pg_namespace.nspname = $2 \
LIMIT 1";

const LOOK_UP_NO_SCHEMA: &str = "\
SELECT pg_type.oid, pg_type.typarray FROM pg_type \
WHERE pg_type.oid = quote_ident($1)::regtype::oid \
LIMIT 1";

async fn lookup_type(
    schema: Option<String>,
    type_name: String,
    raw_connection: &Client,
) -> QueryResult<(u32, u32)> {
    use xitca_postgres::AsyncLendingIterator;
    match schema {
        Some(ref schema) => {
            let stmt = raw_connection
                .prepare(LOOK_UP, &[])
                .await
                .map_err(ErrorHelper)?;
            raw_connection
                .query(&stmt, &[&type_name, schema])
                .map_err(ErrorHelper)?
                .try_next()
                .await
                .map_err(ErrorHelper)?
                .ok_or_else(|| Error::NotFound)
                .map(|r| (r.get(0), r.get(1)))
        }
        None => {
            let stmt = raw_connection
                .prepare(LOOK_UP_NO_SCHEMA, &[])
                .await
                .map_err(ErrorHelper)?;
            raw_connection
                .query(&stmt, &[&type_name])
                .map_err(ErrorHelper)?
                .try_next()
                .await
                .map_err(ErrorHelper)?
                .ok_or_else(|| Error::NotFound)
                .map(|r| (r.get(0), r.get(1)))
        }
    }
}

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
) -> Option<()> {
    let serialized_oid = binds
        .get_mut(bind_index)?
        .as_mut()?
        .get_mut(byte_index..)?
        .first_chunk_mut::<4>()?;
    *serialized_oid = real_oids
        .get(&u32::from_be_bytes(*serialized_oid))?
        .to_be_bytes();
    Some(())
}

impl PoolableConnection for AsyncPgConnection {
    fn is_broken(&mut self) -> bool {
        use diesel_async::TransactionManager;

        Self::TransactionManager::is_broken_transaction_manager(self) || self.conn.closed()
    }
}
