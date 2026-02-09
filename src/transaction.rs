use diesel::QueryResult;
use xitca_postgres::pool::PoolConnection;

use crate::{connection::Meta, error};

type _Transaction<'a, 'b> = xitca_postgres::transaction::Transaction<'a, PoolConnection<'b>>;

pub struct Transaction<'a, 'b> {
    pub(crate) tx: _Transaction<'a, 'b>,
    pub(crate) meta: &'a Meta,
}

impl<'b> Transaction<'_, 'b> {
    pub(crate) async fn run<'a, F, T>(
        tx: _Transaction<'a, 'b>,
        meta: &'a Meta,
        exec: F,
    ) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut Transaction<'_, '_>) -> QueryResult<T>,
    {
        let mut tx = Transaction { tx, meta };
        match exec(&mut tx).await {
            Ok(res) => {
                tx.tx.commit().await.map_err(error::into_error)?;
                Ok(res)
            }
            Err(e) => {
                tx.tx.rollback().await.map_err(error::into_error)?;
                Err(e)
            }
        }
    }

    /// start a scoped transaction through an unnamed save point.
    /// the transaction would implicitly begin before the async closure runs and end after the closure finish.
    /// when the async closure returns with Ok path the transaction would implicitly release the savepoint and returns with the value of OK if the release succeed.
    /// when the async closure returns with Err path the transaction would implicitly rollback the savepoint and returns with the value of Err if the rollback succeed.
    /// savepoint release and rollback error has higher priority than the outcome of async closure.
    ///
    /// # Examples
    /// ```rust
    /// # use diesel::{prelude::*, result::QueryResult};
    /// # use xitca_postgres_diesel::{AsyncPgConnection, RunQueryDsl};
    /// # async fn tx(conn: &AsyncPgConnection) -> QueryResult<()> {
    /// # table! {
    /// #   users {
    /// #       id -> Integer,
    /// #       name -> Text,
    /// #   }
    /// # }
    /// # #[derive(Queryable, Selectable)]
    /// # #[diesel(table_name = users)]
    /// # struct User {
    /// #     id: i32,
    /// #     name: String,
    /// # }
    /// // start a trasaction and run query
    /// let res = conn.transaction(async |tx| {
    ///     // start a nested transaction with savepoint
    ///     tx.transaction(async |sp| {
    ///         users::table.filter(users::id.gt(0)).load::<User>(sp).await
    ///     }).await
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, T>(&mut self, exec: F) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut Transaction<'_, '_>) -> QueryResult<T>,
    {
        let tx = self.tx.transaction().await.map_err(error::into_error)?;
        Transaction::run(tx, self.meta, exec).await
    }
}
