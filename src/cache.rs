use core::{
    future::{Future, Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};

use diesel::{
    QueryResult,
    connection::statement_cache::{MaybeCached, StatementCallbackReturnType},
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) struct CallbackHelper<F>(pub(crate) F);

pub enum PrepareFuture<'a, C, S> {
    Left(Ready<QueryResult<(MaybeCached<'a, S>, C)>>),
    Right(BoxFuture<'a, QueryResult<(MaybeCached<'a, S>, C)>>),
}

impl<'a, C, S> Future for PrepareFuture<'a, C, S> {
    type Output = QueryResult<(MaybeCached<'a, S>, C)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Left(fut) => Pin::new(fut).poll(cx),
            Self::Right(fut) => Pin::new(fut).poll(cx),
        }
    }
}

impl<S, F, C> StatementCallbackReturnType<S, C> for CallbackHelper<F>
where
    F: Future<Output = QueryResult<(S, C)>> + Send,
    S: 'static,
{
    type Return<'a> = PrepareFuture<'a, C, S>;

    fn from_error<'a>(e: diesel::result::Error) -> Self::Return<'a> {
        PrepareFuture::Left(ready(Err(e)))
    }

    fn map_to_no_cache<'a>(self) -> Self::Return<'a>
    where
        Self: 'a,
    {
        PrepareFuture::Right(Box::pin(async {
            self.0
                .await
                .map(|(stmt, conn)| (MaybeCached::CannotCache(stmt), conn))
        }))
    }

    fn map_to_cache(stmt: &mut S, conn: C) -> Self::Return<'_> {
        PrepareFuture::Left(ready(Ok((MaybeCached::Cached(stmt), conn))))
    }

    fn register_cache<'a>(
        self,
        callback: impl FnOnce(S) -> &'a mut S + Send + 'a,
    ) -> Self::Return<'a>
    where
        Self: 'a,
    {
        PrepareFuture::Right(Box::pin(async {
            self.0
                .await
                .map(|(stmt, conn)| (MaybeCached::Cached(callback(stmt)), conn))
        }))
    }
}

pub(crate) struct QueryFragmentHelper<'a> {
    pub(crate) sql: &'a str,
    pub(crate) safe_to_cache: bool,
}
