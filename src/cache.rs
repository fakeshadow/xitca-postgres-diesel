use core::hash::Hash;

use std::collections::HashMap;

use diesel::{
    backend::Backend,
    connection::{
        statement_cache::{MaybeCached, PrepareForCache, StatementCacheKey},
        Instrumentation, InstrumentationEvent,
    },
    result::QueryResult,
};

#[derive(Default)]
pub struct StmtCache<DB: Backend, S> {
    cache: HashMap<StatementCacheKey<DB>, S>,
}

#[async_trait::async_trait]
pub trait PrepareCallback<S, M>: Sized {
    async fn prepare(
        self,
        sql: &str,
        metadata: &[M],
        is_for_cache: PrepareForCache,
    ) -> QueryResult<(S, Self)>;
}

impl<S, DB: Backend> StmtCache<DB, S> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub async fn cached_prepared_statement<'a, F>(
        &'a mut self,
        cache_key: StatementCacheKey<DB>,
        sql: String,
        is_query_safe_to_cache: bool,
        metadata: &[DB::TypeMetadata],
        prepare_fn: F,
        instrumentation: &std::sync::Mutex<Option<Box<dyn Instrumentation>>>,
    ) -> QueryResult<(MaybeCached<'a, S>, F)>
    where
        S: Send,
        DB::QueryBuilder: Default,
        DB::TypeMetadata: Clone + Send + Sync,
        F: PrepareCallback<S, DB::TypeMetadata> + Send + 'a,
        StatementCacheKey<DB>: Hash + Eq,
    {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        if !is_query_safe_to_cache {
            let metadata = metadata.to_vec();
            return Box::pin(async move {
                prepare_fn
                    .prepare(&sql, &metadata, PrepareForCache::No)
                    .await
                    .map(|stmt| (MaybeCached::CannotCache(stmt.0), stmt.1))
            })
            .await;
        }

        match self.cache.entry(cache_key) {
            Occupied(entry) => Ok((MaybeCached::Cached(entry.into_mut()), prepare_fn)),
            Vacant(entry) => {
                let metadata = metadata.to_vec();
                instrumentation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .on_connection_event(InstrumentationEvent::cache_query(&sql));
                Box::pin(async move {
                    prepare_fn
                        .prepare(&sql, &metadata, PrepareForCache::Yes)
                        .await
                        .map(|stmt| (MaybeCached::Cached(entry.insert(stmt.0)), stmt.1))
                })
                .await
            }
        }
    }
}
