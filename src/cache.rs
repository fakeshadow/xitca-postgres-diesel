use core::future::{Future, Ready, ready};

use diesel::{
    QueryResult,
    connection::statement_cache::{MaybeCached, StatementCallbackReturnType},
};
use futures_util::future::{BoxFuture, Either, FutureExt, TryFutureExt};

pub(crate) struct CallbackHelper<F>(pub(crate) F);

type PrepareFuture<'a, C, S> = Either<
    Ready<QueryResult<(MaybeCached<'a, S>, C)>>,
    BoxFuture<'a, QueryResult<(MaybeCached<'a, S>, C)>>,
>;

impl<S, F, C> StatementCallbackReturnType<S, C> for CallbackHelper<F>
where
    F: Future<Output = QueryResult<(S, C)>> + Send,
    S: 'static,
{
    type Return<'a> = PrepareFuture<'a, C, S>;

    fn from_error<'a>(e: diesel::result::Error) -> Self::Return<'a> {
        Either::Left(ready(Err(e)))
    }

    fn map_to_no_cache<'a>(self) -> Self::Return<'a>
    where
        Self: 'a,
    {
        Either::Right(
            self.0
                .map_ok(|(stmt, conn)| (MaybeCached::CannotCache(stmt), conn))
                .boxed(),
        )
    }

    fn map_to_cache(stmt: &mut S, conn: C) -> Self::Return<'_> {
        Either::Left(ready(Ok((MaybeCached::Cached(stmt), conn))))
    }

    fn register_cache<'a>(
        self,
        callback: impl FnOnce(S) -> &'a mut S + Send + 'a,
    ) -> Self::Return<'a>
    where
        Self: 'a,
    {
        Either::Right(
            self.0
                .map_ok(|(stmt, conn)| (MaybeCached::Cached(callback(stmt)), conn))
                .boxed(),
        )
    }
}

pub(crate) struct QueryFragmentHelper {
    pub(crate) sql: String,
    pub(crate) safe_to_cache: bool,
}
