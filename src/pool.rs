//! placeholder module for testing pooling integration with diesel-async

use core::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use std::{collections::VecDeque, sync::Mutex};

use diesel::ConnectionError;
use diesel_async::pooled_connection::PoolableConnection;
use tokio::sync::{Semaphore, SemaphorePermit};
use xitca_postgres::Config;

pub struct PoolBuilder<C> {
    cap: usize,
    _conn: PhantomData<C>,
}

impl<C> Default for PoolBuilder<C> {
    fn default() -> Self {
        Self {
            cap: 1,
            _conn: PhantomData,
        }
    }
}

impl<C> PoolBuilder<C> {
    pub fn capacity(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    pub fn build(self, config: impl Into<String>) -> Result<Pool<C>, xitca_postgres::Error> {
        let config = config.into();
        Config::try_from(config.as_str())?;
        Ok(Pool {
            conn: Mutex::new(VecDeque::with_capacity(self.cap)),
            permits: Semaphore::new(self.cap),
            url: config,
        })
    }
}

pub struct Pool<C> {
    conn: Mutex<VecDeque<C>>,
    permits: Semaphore,
    url: String,
}

impl<C> Pool<C>
where
    C: PoolableConnection,
{
    pub fn builder() -> PoolBuilder<C> {
        PoolBuilder::default()
    }

    pub async fn get(&self) -> Result<PoolConnection<C>, ConnectionError> {
        let _permit = self.permits.acquire().await.unwrap();
        let conn = self.conn.lock().unwrap().pop_front();
        let conn = match conn {
            Some(conn) => conn,
            None => C::establish(&self.url).await?,
        };

        Ok(PoolConnection {
            conn: Some(conn),
            pool: self,
            _permit,
        })
    }
}

pub struct PoolConnection<'a, C>
where
    C: PoolableConnection,
{
    conn: Option<C>,
    pool: &'a Pool<C>,
    _permit: SemaphorePermit<'a>,
}

impl<C> Deref for PoolConnection<'_, C>
where
    C: PoolableConnection,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl<C> DerefMut for PoolConnection<'_, C>
where
    C: PoolableConnection,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

impl<C> Drop for PoolConnection<'_, C>
where
    C: PoolableConnection,
{
    fn drop(&mut self) {
        let mut conn = self.conn.take().unwrap();
        if conn.is_broken() {
            return;
        }

        self.pool.conn.lock().unwrap().push_back(conn);
    }
}

#[cfg(test)]
mod test {
    use diesel_async::{
        pooled_connection::{bb8, AsyncDieselConnectionManager},
        RunQueryDsl,
    };

    #[tokio::test]
    async fn bb8() {
        let pool = bb8::Pool::<crate::AsyncPgConnection>::builder()
            .build(AsyncDieselConnectionManager::new(
                "postgres://postgres:postgres@localhost:5432/postgres",
            ))
            .await
            .unwrap();

        let mut conn = pool.get().await.unwrap();

        diesel::sql_query("SELECT 1")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pool() {
        let pool = super::Pool::<diesel_async::pg::AsyncPgConnection>::builder()
            .build("postgres://postgres:postgres@localhost:5432/postgres")
            .unwrap();
        let mut conn = pool.get().await.unwrap();

        diesel::sql_query("SELECT 1")
            .execute(&mut conn)
            .await
            .unwrap();
    }
}
