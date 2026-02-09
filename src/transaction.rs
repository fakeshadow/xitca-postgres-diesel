use diesel::QueryResult;
use xitca_postgres::{pool::PoolConnection, transaction::Transaction};

use crate::{connection::Meta, error};

pub struct TransactionConnection<'a, 'b> {
    pub(crate) tx: Transaction<'a, PoolConnection<'b>>,
    pub(crate) meta: &'a Meta,
}

impl<'b> TransactionConnection<'_, 'b> {
    pub(crate) async fn run<'a, F, T>(
        tx: Transaction<'a, PoolConnection<'b>>,
        meta: &'a Meta,
        exec: F,
    ) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut TransactionConnection<'_, '_>) -> QueryResult<T>,
    {
        let mut tx = TransactionConnection { tx, meta };
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
    pub async fn transaction<F, T>(&mut self, exec: F) -> QueryResult<T>
    where
        F: AsyncFnOnce(&mut TransactionConnection<'_, '_>) -> QueryResult<T>,
    {
        let tx = self.tx.transaction().await.map_err(error::into_error)?;
        TransactionConnection::run(tx, self.meta, exec).await
    }
}
