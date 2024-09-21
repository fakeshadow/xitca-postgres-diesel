use core::{any::TypeId, future::Future};

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
        query_id: Option<TypeId>,
        sql: String,
        is_query_safe_to_cache: bool,
        metadata: &[PgTypeMetadata],
        prepare_fn: &Arc<Client>,
        instrumentation: &std::sync::Mutex<dyn Instrumentation>,
    ) -> QueryResult<Statement> {
        if !is_query_safe_to_cache {
            return Box::pin(prepare_fn.prepare(&sql, metadata)).await;
        }

        let (cache_key, opt) = match query_id {
            Some(id) => (StatementCacheKey::Type(id), Some((sql, metadata))),
            None => (
                StatementCacheKey::Sql {
                    sql,
                    bind_types: metadata.to_owned(),
                },
                None,
            ),
        };

        if let Some(stmt) = self.cache.get(&cache_key) {
            return Ok(stmt.clone());
        }

        Box::pin(async move {
            let (sql, meta) = match cache_key {
                StatementCacheKey::Type(ref _id) => {
                    opt.as_ref().map(|(sql, meta)| (sql, *meta)).unwrap()
                }
                StatementCacheKey::Sql {
                    ref sql,
                    ref bind_types,
                } => (sql, &**bind_types),
            };

            instrumentation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .on_connection_event(InstrumentationEvent::cache_query(sql));

            let stmt = prepare_fn.prepare(sql, meta).await?;
            self.cache.insert(cache_key, stmt.clone());
            Ok(stmt)
        })
        .await
    }
}
