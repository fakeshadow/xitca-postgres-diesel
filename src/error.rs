use core::{
    future::{poll_fn, Future},
    pin::Pin,
    task::{ready, Poll},
};

use std::sync::{Arc, Mutex};

use diesel::result::{ConnectionError, DatabaseErrorInformation, Error};
use tokio::task::JoinHandle;

use crate::BoxFuture;

#[derive(Clone)]
pub(crate) struct ErrorJoiner {
    handle: Option<Arc<std::sync::Mutex<JoinerInner>>>,
}

enum JoinerInner {
    Handle(JoinHandle<xitca_postgres::Error>),
    Error(xitca_postgres::Error),
}

impl ErrorJoiner {
    pub(crate) fn new(handle: Option<JoinHandle<xitca_postgres::Error>>) -> Self {
        Self {
            handle: handle.map(|handle| Arc::new(Mutex::new(JoinerInner::Handle(handle)))),
        }
    }

    // transform xitca_postgres::Error to diesel::result::Error on certain condition.
    #[cold]
    #[inline(never)]
    pub(crate) fn join(&self, e: xitca_postgres::Error) -> BoxFuture<'_, Error> {
        Box::pin(async move {
            // when xitca_postgres emit driver shutdown error it means it's Driver
            // task has shutdown already. in this case just await for the driver error
            // to show up from join handle and replace client's error type.
            if e.is_driver_down() {
                if let Some(ref inner) = self.handle {
                    return poll_fn(|cx| {
                        let mut inner = inner.lock().unwrap();
                        match *inner {
                            JoinerInner::Error(ref e) => Poll::Ready(into_error_ref(e)),
                            JoinerInner::Handle(ref mut handle) => {
                                let err = ready!(Pin::new(handle).poll(cx))
                                    .expect("Driver's task must not panic");
                                let e = into_error_ref(&err);
                                let _ = core::mem::replace(&mut *inner, JoinerInner::Error(err));
                                Poll::Ready(e)
                            }
                        }
                    })
                    .await;
                }
            }
            into_error_ref(&e)
        })
    }
}

pub(crate) fn into_connection_error(e: xitca_postgres::Error) -> ConnectionError {
    ConnectionError::CouldntSetupConfiguration(into_error(e))
}

pub(crate) fn into_error(e: xitca_postgres::Error) -> Error {
    into_error_ref(&e)
}

fn into_error_ref(e: &xitca_postgres::Error) -> Error {
    use diesel::result::DatabaseErrorKind::*;

    match e.downcast_ref::<xitca_postgres::error::DbError>() {
        Some(e) => {
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
            Error::DatabaseError(kind, Box::new(PostgresDbErrorWrapper(e.clone())) as _)
        }
        None => Error::DatabaseError(UnableToSendCommand, Box::new(e.to_string())),
    }
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
