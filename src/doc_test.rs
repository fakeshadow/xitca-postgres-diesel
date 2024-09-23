#[allow(unused_imports)]
use diesel::prelude::{
    AsChangeset, ExpressionMethods, Identifiable, IntoSql, QueryDsl, QueryResult, Queryable,
    QueryableByName,
};
use diesel_async::{AsyncConnection, AsyncEstablish, AsyncTransaction};

use xitca_postgres_diesel::AsyncPgConnection;
#[allow(dead_code)]
type DB = diesel::pg::Pg;
#[allow(dead_code)]
type DbConnection = AsyncPgConnection;

fn database_url() -> String {
    database_url_from_env("PG_DATABASE_URL")
}

async fn connection_no_transaction() -> AsyncPgConnection {
    let connection_url = database_url();
    AsyncPgConnection::establish(&connection_url).await.unwrap()
}

async fn connection_no_data() -> AsyncPgConnection {
    let mut connection = connection_no_transaction().await;
    connection.begin_test_transaction().await.unwrap();
    connection
}

async fn create_tables(connection: &mut AsyncPgConnection) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
    diesel::sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')")
        .execute(connection)
        .await
        .unwrap();

    diesel::sql_query(
        "CREATE TEMPORARY TABLE animals (
                id SERIAL PRIMARY KEY,
                species VARCHAR NOT NULL,
                legs INTEGER NOT NULL,
                name VARCHAR
            )",
    )
    .execute(connection)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO animals (species, legs, name) VALUES
                               ('dog', 4, 'Jack'),
                               ('spider', 8, null)",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TEMPORARY TABLE posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title VARCHAR NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO posts (user_id, title) VALUES
                (1, 'My first post'),
                (1, 'About Rust'),
                (2, 'My first post too')",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TEMPORARY TABLE comments (
                id SERIAL PRIMARY KEY,
                post_id INTEGER NOT NULL,
                body VARCHAR NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO comments (post_id, body) VALUES
                (1, 'Great post'),
                (2, 'Yay! I am learning Rust'),
                (3, 'I enjoyed your post')",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TEMPORARY TABLE brands (
                id SERIAL PRIMARY KEY,
                color VARCHAR NOT NULL DEFAULT 'Green',
                accent VARCHAR DEFAULT 'Blue'
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

#[allow(dead_code)]
async fn establish_connection() -> AsyncPgConnection {
    let mut connection = connection_no_data().await;
    create_tables(&mut connection).await;
    connection
}

fn database_url_from_env(backend_specific_env_var: &str) -> String {
    use std::env;

    env::var(backend_specific_env_var)
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests")
}

mod schema {
    use diesel::prelude::*;

    table! {
        animals {
            id -> Integer,
            species -> VarChar,
            legs -> Integer,
            name -> Nullable<VarChar>,
        }
    }

    table! {
        comments {
            id -> Integer,
            post_id -> Integer,
            body -> VarChar,
        }
    }

    table! {
        posts {
            id -> Integer,
            user_id -> Integer,
            title -> VarChar,
        }
    }

    table! {
        users {
            id -> Integer,
            name -> VarChar,
        }
    }

    table! {
        brands {
            id -> Integer,
            color -> VarChar,
            accent -> Nullable<VarChar>,
        }
    }

    joinable!(posts -> users (user_id));
    allow_tables_to_appear_in_same_query!(animals, comments, posts, users);
}
