use diesel::{ConnectionError, pg::PgMetadataCache, result::QueryResult};
use xitca_postgres::pool::Pool;

use crate::{error, transaction::TransactionConnection};

pub struct AsyncPgConnection {
    pub(crate) pool: Pool,
    pub(crate) meta: Meta,
}

pub(crate) type Meta = tokio::sync::Mutex<PgMetadataCache>;

impl AsyncPgConnection {
    pub async fn establish(url: &str) -> Result<Self, ConnectionError> {
        let pool = Pool::builder(url)
            .cache_size(32)
            .build()
            .map_err(error::into_connection_error)?;

        Ok(Self {
            pool,
            meta: Default::default(),
        })
    }

    /// start a transction with given async closure.
    /// the transaction would implicitly begin before the async closure runs and end after the closure finish.
    /// when the async closure returns with Ok path the transaction would implicitly commit and returns with the value of OK if the commit succeed.
    /// when the async closure returns with Err path the transaction would implicitly rollback and returns with the value of Err if the rollback succeed.
    /// commit and rollback error has higher priority than the outcome of async closure.
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
    ///     users::table.filter(users::id.gt(0)).load::<User>(tx).await
    /// }).await?;
    ///
    /// // start a transaction and actively rollback afterwards
    /// let res = conn.transaction(async |_| {
    ///     // all error case would result in rollback of transction but in this case
    ///     // it's best to specific it to this variant for clear reasoning
    ///     Err(diesel::result::Error::RollbackTransaction)
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, T>(&self, exec: F) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut TransactionConnection<'_, '_>) -> QueryResult<T>,
    {
        let mut conn = self.pool.get().await.map_err(error::into_error)?;
        let tx = conn.transaction().await.map_err(error::into_error)?;
        TransactionConnection::run(tx, &self.meta, exec).await
    }
}

fn _assert_send<F: Send>(_: F) {}

fn _send_check(conn: &AsyncPgConnection) {
    let tx = conn.transaction(async |_| Err::<(), _>(diesel::result::Error::RollbackTransaction));
    _assert_send(tx);
}
