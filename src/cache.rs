use core::future::Future;

use std::{collections::HashMap, sync::Arc};

use diesel::{
    connection::{statement_cache::StatementCacheKey, Instrumentation, InstrumentationEvent},
    pg::{Pg, PgTypeMetadata},
    result::QueryResult,
};
use xitca_postgres::Client;

use crate::Statement;

#[derive(Default)]
pub struct StmtCache {
    cache: HashMap<StatementCacheKey<Pg>, Statement>,
}

pub trait PrepareCallback {
    fn prepare(
        &self,
        sql: &str,
        metadata: &[PgTypeMetadata],
    ) -> impl Future<Output = QueryResult<Statement>> + Send;
}

impl StmtCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub async fn cached_prepared_statement(
        &mut self,
        cache_key: StatementCacheKey<Pg>,
        sql: &str,
        is_query_safe_to_cache: bool,
        metadata: &[PgTypeMetadata],
        prepare_fn: &Arc<Client>,
        instrumentation: &std::sync::Mutex<Option<Box<dyn Instrumentation>>>,
    ) -> QueryResult<Statement> {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        if !is_query_safe_to_cache {
            let metadata = metadata.to_vec();
            return Box::pin(prepare_fn.prepare(sql, &metadata)).await;
        }

        match self.cache.entry(cache_key) {
            Occupied(entry) => Ok(entry.into_mut().clone()),
            Vacant(entry) => {
                let metadata = metadata.to_vec();
                instrumentation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .on_connection_event(InstrumentationEvent::cache_query(sql));
                Box::pin(async move {
                    prepare_fn.prepare(sql, &metadata).await.inspect(|stmt| {
                        entry.insert(stmt.clone());
                    })
                })
                .await
            }
        }
    }
}
