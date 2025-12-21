use core::{
    future::{Future, poll_fn},
    mem,
    pin::Pin,
    task::{Poll, ready},
};

use std::sync::Mutex;

use diesel::result::{ConnectionError, DatabaseErrorInformation, Error};
use tokio::task::JoinHandle;

use crate::BoxFuture;

type JoinOuput = Result<(), xitca_postgres::Error>;

pub(crate) struct ErrorJoiner {
    inner: Mutex<JoinerInner>,
}

enum JoinerInner {
    Handle(JoinHandle<JoinOuput>),
    Output(Option<xitca_postgres::Error>),
}

impl ErrorJoiner {
    pub(crate) fn new(handle: Option<JoinHandle<JoinOuput>>) -> Self {
        let inner = match handle {
            Some(handle) => JoinerInner::Handle(handle),
            None => JoinerInner::Output(None),
        };

        Self {
            inner: Mutex::new(inner),
        }
    }

    // transform xitca_postgres::Error to diesel::result::Error on certain condition.
    #[cold]
    #[inline(never)]
    pub(crate) fn join(&self, mut e: xitca_postgres::Error) -> BoxFuture<'_, Error> {
        Box::pin(async move {
            // when xitca_postgres emit driver shutdown error it means it's Driver
            // task has shutdown already. in this case just await for the driver error
            // to show up from join handle and replace client's error type.
            if e.is_driver_down()
                && let Some(err) = poll_fn(|cx| {
                    let mut inner = self.inner.lock().unwrap();
                    loop {
                        match *inner {
                            JoinerInner::Output(ref mut err) => return Poll::Ready(err.take()),
                            JoinerInner::Handle(ref mut handle) => {
                                let res = ready!(Pin::new(handle).poll(cx))
                                    .expect("driver task must not panic");
                                let _ = mem::replace(&mut *inner, JoinerInner::Output(res.err()));
                            }
                        }
                    }
                })
                .await
            {
                e = err;
            }

            into_error(e)
        })
    }
}

pub(crate) fn into_connection_error(e: xitca_postgres::Error) -> ConnectionError {
    ConnectionError::CouldntSetupConfiguration(into_error(e))
}

pub(crate) fn into_error(e: xitca_postgres::Error) -> Error {
    use diesel::result::DatabaseErrorKind::*;

    if e.is_driver_down() {
        return Error::DatabaseError(ClosedConnection, Box::new(e.to_string()));
    }

    if let Some(e) = e.downcast_ref::<xitca_postgres::error::DbError>() {
        use xitca_postgres::error::SqlState;
        let kind = match *e.code() {
            SqlState::UNIQUE_VIOLATION => UniqueViolation,
            SqlState::FOREIGN_KEY_VIOLATION => ForeignKeyViolation,
            SqlState::T_R_SERIALIZATION_FAILURE => SerializationFailure,
            SqlState::READ_ONLY_SQL_TRANSACTION => ReadOnlyTransaction,
            SqlState::NOT_NULL_VIOLATION => NotNullViolation,
            SqlState::CHECK_VIOLATION => CheckViolation,
            _ => Unknown,
        };
        return Error::DatabaseError(kind, Box::new(PostgresDbErrorWrapper(e.clone())) as _);
    }

    Error::DatabaseError(UnableToSendCommand, Box::new(e.to_string()))
}

struct PostgresDbErrorWrapper(xitca_postgres::error::DbError);

impl DatabaseErrorInformation for PostgresDbErrorWrapper {
    fn message(&self) -> &str {
        self.0.message()
    }

    fn details(&self) -> Option<&str> {
        self.0.detail()
    }

    fn hint(&self) -> Option<&str> {
        self.0.hint()
    }

    fn table_name(&self) -> Option<&str> {
        self.0.table()
    }

    fn column_name(&self) -> Option<&str> {
        self.0.column()
    }

    fn constraint_name(&self) -> Option<&str> {
        self.0.constraint()
    }

    fn statement_position(&self) -> Option<i32> {
        use xitca_postgres::error::ErrorPosition;
        self.0.position().map(|e| match e {
            ErrorPosition::Original(position) | ErrorPosition::Internal { position, .. } => {
                *position as i32
            }
        })
    }
}
