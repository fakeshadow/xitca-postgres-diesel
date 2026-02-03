use diesel::{ConnectionError, pg::PgMetadataCache, result::QueryResult};
use xitca_postgres::pool::Pool;

use crate::{TransactionConnection, error};

pub struct AsyncPgConnection {
    pub(crate) pool: Pool,
    pub(crate) meta: Meta,
}

pub(crate) type Meta = tokio::sync::Mutex<PgMetadataCache>;

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

    /// start a transction with given async closure.
    /// the transaction would implicitly begin before the async closure runs and end after the closure finish.
    /// when the async closure returns with Ok path the transaction would implicitly commit and returns with the value of OK if the commit succeed.
    /// when the async closure returns with Err path the transaction would implicitly rollback and returns with the value of if the rollback succeed.
    /// commit and rollback error has higher priority than the outcome of async closure.
    pub async fn transaction<F, T>(&self, exec: F) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut TransactionConnection<'_>) -> QueryResult<T>,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(error::into_error)?
            .transaction_owned()
            .await
            .map_err(error::into_error)?;

        let mut conn = TransactionConnection {
            conn,
            meta: &self.meta,
        };

        match exec(&mut conn).await {
            Ok(res) => {
                conn.conn.commit().await.map_err(error::into_error)?;
                Ok(res)
            }
            Err(e) => {
                conn.conn.rollback().await.map_err(error::into_error)?;
                Err(e)
            }
        }
    }
}
