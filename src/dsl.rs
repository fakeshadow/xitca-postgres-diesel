use diesel::{
    deserialize::FromSqlRow,
    pg::Pg,
    query_builder::{AsQuery, QueryFragment, QueryId},
    query_dsl::CompatibleType,
    result::{Error, QueryResult},
};
use xitca_postgres::{Execute, RowStreamOwned, iter::AsyncLendingIterator};

use crate::{
    connection::AsyncPgConnection,
    error,
    pre_execute::{BindValueIter, PreExecute},
    row::PgRow,
    transaction::Transaction,
};

/// async version of [`diesel::query_dsl::RunQueryDsl`]
pub trait RunQueryDsl<C>
where
    C: _RunQueryDsl<Self> + Send,
    Self: AsQuery + Send + Sized,
    Self::Query: QueryFragment<Pg> + QueryId + Send,
{
    /// async version of [`diesel::query_dsl::RunQueryDsl::execute`]
    #[inline]
    fn execute(self, conn: C) -> impl Future<Output = QueryResult<usize>> + Send {
        conn._execute(self)
    }

    /// async version of [`diesel::query_dsl::RunQueryDsl::load`]
    fn load<U>(self, conn: C) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let mut res = Vec::new();
            self.load_into(conn, &mut res).await.map(|_| res)
        }
    }

    /// alternative version of [`diesel::query_dsl::RunQueryDsl::load_iter`] where the iteration
    /// happens inside the method with given collection type. when function returns sucessfully
    /// the given collection type would be populated with type instance converted from row data
    fn load_into<U, R>(
        self,
        conn: C,
        collection: &mut R,
    ) -> impl Future<Output = QueryResult<()>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        R: Extend<U> + Send,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let stream = conn._load(self).await?;
            try_collect_into(stream, collection).await
        }
    }

    /// async version of [`diesel::query_dsl::RunQueryDsl::get_result`]
    fn get_result<U>(self, conn: C) -> impl Future<Output = QueryResult<U>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
        Self::SqlType: CompatibleType<U, Pg>,
    {
        async {
            let mut stream = conn._load(self).await?;
            try_next(&mut stream).await?.ok_or_else(|| Error::NotFound)
        }
    }

    /// async version of [`diesel::query_dsl::RunQueryDsl::get_results`]
    #[inline]
    fn get_results<U>(self, conn: C) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send,
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
        Self: diesel::query_dsl::methods::LimitDsl,
        diesel::dsl::Limit<Self>: RunQueryDsl<C>,
        C: _RunQueryDsl<diesel::dsl::Limit<Self>>,
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
}

impl<Q> RunQueryDsl<&mut Transaction<'_, '_>> for Q
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
}

#[doc(hidden)]
pub trait _RunQueryDsl<Q>
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    fn _execute(self, query: Q) -> impl Future<Output = QueryResult<usize>> + Send;

    fn _load(self, query: Q) -> impl Future<Output = QueryResult<RowStreamOwned>> + Send;
}

impl<Q> _RunQueryDsl<Q> for &AsyncPgConnection
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    async fn _execute(self, query: Q) -> QueryResult<usize> {
        let res = {
            let mut pool_conn = self.pool.get().await.map_err(error::into_error)?;
            let (stmt, bind) = pool_conn.pre_execute(query, &self.meta).await?;
            stmt.bind(BindValueIter::from(&bind)).execute(&pool_conn)
        }
        .await
        .map_err(error::into_error)?;

        Ok(res as _)
    }

    async fn _load(self, query: Q) -> QueryResult<RowStreamOwned> {
        let mut pool_conn = self.pool.get().await.map_err(error::into_error)?;
        let (stmt, bind) = pool_conn.pre_execute(query, &self.meta).await?;
        stmt.bind(BindValueIter::from(&bind))
            .into_owned()
            .query(&pool_conn)
            .await
            .map_err(error::into_error)
    }
}

impl<Q> _RunQueryDsl<Q> for &mut Transaction<'_, '_>
where
    Q: AsQuery + Send,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
{
    async fn _execute(self, query: Q) -> QueryResult<usize> {
        let (stmt, bind) = self.tx.pre_execute(query, self.meta).await?;
        let res = stmt
            .bind(BindValueIter::from(&bind))
            .execute(&self.tx)
            .await
            .map_err(error::into_error)?;
        Ok(res as _)
    }

    async fn _load(self, query: Q) -> QueryResult<RowStreamOwned> {
        let (stmt, bind) = self.tx.pre_execute(query, self.meta).await?;
        stmt.bind(BindValueIter::from(&bind))
            .into_owned()
            .query(&self.tx)
            .await
            .map_err(error::into_error)
    }
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
