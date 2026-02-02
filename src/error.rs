use diesel::result::{ConnectionError, DatabaseErrorInformation, Error};

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
