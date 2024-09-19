use diesel::backend::Backend;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryBuilder, QueryFragment};
use diesel::QueryResult;
use diesel_async::{AsyncConnection, TransactionManager};
use scoped_futures::ScopedBoxFuture;

use crate::transaction_manager::AnsiTransactionManager;

/// Used to build a transaction, specifying additional details.
///
/// This struct is returned by [`AsyncPgConnection::build_transaction`].
/// See the documentation for methods on this struct for usage examples.
/// See [the PostgreSQL documentation for `SET TRANSACTION`][pg-docs]
/// for details on the behavior of each option.
///
/// [`AsyncPgConnection::build_transaction`]: super::AsyncPgConnection::build_transaction()
/// [pg-docs]: https://www.postgresql.org/docs/current/static/sql-set-transaction.html
#[must_use = "Transaction builder does nothing unless you call `run` on it"]
pub struct TransactionBuilder<'a, C> {
    connection: &'a mut C,
    isolation_level: Option<IsolationLevel>,
    read_mode: Option<ReadMode>,
    deferrable: Option<Deferrable>,
}

impl<'a, C> TransactionBuilder<'a, C>
where
    C: AsyncConnection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    pub(crate) fn new(connection: &'a mut C) -> Self {
        Self {
            connection,
            isolation_level: None,
            read_mode: None,
            deferrable: None,
        }
    }

    /// Makes the transaction `READ ONLY`
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// # use diesel::sql_query;
    /// use diesel_async::RunQueryDsl;
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # diesel::table! {
    /// #     users_for_read_only {
    /// #         id -> Integer,
    /// #         name -> Text,
    /// #     }
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use users_for_read_only::table as users;
    /// #     use users_for_read_only::columns::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// #     sql_query("CREATE TABLE IF NOT EXISTS users_for_read_only (
    /// #       id SERIAL PRIMARY KEY,
    /// #       name TEXT NOT NULL
    /// #     )").execute(conn).await?;
    /// conn.build_transaction()
    ///     .read_only()
    ///     .run::<_, diesel::result::Error, _>(|conn| Box::pin(async move {
    ///         let read_attempt = users.select(name).load::<String>(conn).await;
    ///         assert!(read_attempt.is_ok());
    ///
    ///         let write_attempt = diesel::insert_into(users)
    ///             .values(name.eq("Ruby"))
    ///             .execute(conn)
    ///             .await;
    ///         assert!(write_attempt.is_err());
    ///
    ///         Ok(())
    ///     }) as _).await?;
    /// #     sql_query("DROP TABLE users_for_read_only").execute(conn).await?;
    /// #     Ok(())
    /// # }
    /// ```
    pub fn read_only(mut self) -> Self {
        self.read_mode = Some(ReadMode::ReadOnly);
        self
    }

    /// Makes the transaction `READ WRITE`
    ///
    /// This is the default, unless you've changed the
    /// `default_transaction_read_only` configuration parameter.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// # use diesel::result::Error::RollbackTransaction;
    /// # use diesel::sql_query;
    /// use diesel_async::RunQueryDsl;
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     assert_eq!(run_test().await, Err(RollbackTransaction));
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .read_write()
    ///     .run(|conn| Box::pin( async move {
    /// #         sql_query("CREATE TABLE IF NOT EXISTS users (
    /// #             id SERIAL PRIMARY KEY,
    /// #             name TEXT NOT NULL
    /// #         )").execute(conn).await?;
    ///         let read_attempt = users.select(name).load::<String>(conn).await;
    ///         assert!(read_attempt.is_ok());
    ///
    ///         let write_attempt = diesel::insert_into(users)
    ///             .values(name.eq("Ruby"))
    ///             .execute(conn)
    ///             .await;
    ///         assert!(write_attempt.is_ok());
    ///
    /// #       Err(RollbackTransaction)
    /// #       /*
    ///         Ok(())
    /// #       */
    ///     }) as _)
    ///     .await
    /// # }
    /// ```
    pub fn read_write(mut self) -> Self {
        self.read_mode = Some(ReadMode::ReadWrite);
        self
    }

    /// Makes the transaction `DEFERRABLE`
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .deferrable()
    ///     .run(|conn| Box::pin(async { Ok(()) }))
    ///     .await
    /// # }
    /// ```
    pub fn deferrable(mut self) -> Self {
        self.deferrable = Some(Deferrable::Deferrable);
        self
    }

    /// Makes the transaction `NOT DEFERRABLE`
    ///
    /// This is the default, unless you've changed the
    /// `default_transaction_deferrable` configuration parameter.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .not_deferrable()
    ///     .run(|conn| Box::pin(async { Ok(()) }) as _)
    ///     .await
    /// # }
    /// ```
    pub fn not_deferrable(mut self) -> Self {
        self.deferrable = Some(Deferrable::NotDeferrable);
        self
    }

    /// Makes the transaction `ISOLATION LEVEL READ COMMITTED`
    ///
    /// This is the default, unless you've changed the
    /// `default_transaction_isolation_level` configuration parameter.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .read_committed()
    ///     .run(|conn| Box::pin(async { Ok(()) }) as _)
    ///     .await
    /// # }
    /// ```
    pub fn read_committed(mut self) -> Self {
        self.isolation_level = Some(IsolationLevel::ReadCommitted);
        self
    }

    /// Makes the transaction `ISOLATION LEVEL REPEATABLE READ`
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .repeatable_read()
    ///     .run(|conn| Box::pin(async { Ok(()) }) as _)
    ///     .await
    /// # }
    /// ```
    pub fn repeatable_read(mut self) -> Self {
        self.isolation_level = Some(IsolationLevel::RepeatableRead);
        self
    }

    /// Makes the transaction `ISOLATION LEVEL SERIALIZABLE`
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("./doc_test.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .serializable()
    ///     .run(|conn| Box::pin(async { Ok(()) }) as _)
    ///     .await
    /// # }
    /// ```
    pub fn serializable(mut self) -> Self {
        self.isolation_level = Some(IsolationLevel::Serializable);
        self
    }

    /// Runs the given function inside of the transaction
    /// with the parameters given to this builder.
    ///
    /// Returns an error if the connection is already inside a transaction,
    /// or if the transaction fails to commit or rollback
    ///
    /// If the transaction fails to commit due to a `SerializationFailure` or a
    /// `ReadOnlyTransaction` a rollback will be attempted. If the rollback succeeds,
    /// the original error will be returned, otherwise the error generated by the rollback
    /// will be returned. In the second case the connection should be considered broken
    /// as it contains a uncommitted unabortable open transaction.
    pub async fn run<'b, T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut C) -> ScopedBoxFuture<'b, 'r, Result<T, E>> + Send + 'a,
        T: 'b,
        E: From<diesel::result::Error> + 'b,
    {
        let mut query_builder = <Pg as Backend>::QueryBuilder::default();
        self.to_sql(&mut query_builder, &Pg)?;
        let sql = query_builder.finish();

        AnsiTransactionManager::begin_transaction_sql(&mut *self.connection, &sql).await?;
        match f(&mut *self.connection).await {
            Ok(value) => {
                AnsiTransactionManager::commit_transaction(&mut *self.connection).await?;
                Ok(value)
            }
            Err(e) => {
                AnsiTransactionManager::rollback_transaction(&mut *self.connection).await?;
                Err(e)
            }
        }
    }
}

impl<'a, C> QueryFragment<Pg> for TransactionBuilder<'a, C> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("BEGIN TRANSACTION");
        if let Some(ref isolation_level) = self.isolation_level {
            isolation_level.walk_ast(out.reborrow())?;
        }
        if let Some(ref read_mode) = self.read_mode {
            read_mode.walk_ast(out.reborrow())?;
        }
        if let Some(ref deferrable) = self.deferrable {
            deferrable.walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl QueryFragment<Pg> for IsolationLevel {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql(" ISOLATION LEVEL ");
        match *self {
            IsolationLevel::ReadCommitted => out.push_sql("READ COMMITTED"),
            IsolationLevel::RepeatableRead => out.push_sql("REPEATABLE READ"),
            IsolationLevel::Serializable => out.push_sql("SERIALIZABLE"),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum ReadMode {
    ReadOnly,
    ReadWrite,
}

impl QueryFragment<Pg> for ReadMode {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        match *self {
            ReadMode::ReadOnly => out.push_sql(" READ ONLY"),
            ReadMode::ReadWrite => out.push_sql(" READ WRITE"),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Deferrable {
    Deferrable,
    NotDeferrable,
}

impl QueryFragment<Pg> for Deferrable {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        match *self {
            Deferrable::Deferrable => out.push_sql(" DEFERRABLE"),
            Deferrable::NotDeferrable => out.push_sql(" NOT DEFERRABLE"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transaction_builder_generates_correct_sql() {
        macro_rules! assert_sql {
            ($query:expr, $sql:expr) => {
                let mut query_builder = <Pg as Backend>::QueryBuilder::default();
                $query.to_sql(&mut query_builder, &Pg).unwrap();
                let sql = query_builder.finish();
                assert_eq!(sql, $sql);
            };
        }

        let database_url =
            dbg!(std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set in order to run tests"));
        let mut conn = crate::AsyncPgConnection::establish(&database_url)
            .await
            .unwrap();

        assert_sql!(conn.build_transaction(), "BEGIN TRANSACTION");
        assert_sql!(
            conn.build_transaction().read_only(),
            "BEGIN TRANSACTION READ ONLY"
        );
        assert_sql!(
            conn.build_transaction().read_write(),
            "BEGIN TRANSACTION READ WRITE"
        );
        assert_sql!(
            conn.build_transaction().deferrable(),
            "BEGIN TRANSACTION DEFERRABLE"
        );
        assert_sql!(
            conn.build_transaction().not_deferrable(),
            "BEGIN TRANSACTION NOT DEFERRABLE"
        );
        assert_sql!(
            conn.build_transaction().read_committed(),
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
        );
        assert_sql!(
            conn.build_transaction().repeatable_read(),
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        );
        assert_sql!(
            conn.build_transaction().serializable(),
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        );
        assert_sql!(
            conn.build_transaction()
                .serializable()
                .deferrable()
                .read_only(),
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"
        );
    }
}
