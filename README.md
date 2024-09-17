an async ORM for postgresql built upon [diesel-async](https://crates.io/crates/diesel-async)

## Usage
`xitca-postgres-diesel` is an extension crate of `diesel-async` by offering connection type
that can hook into `diesel` and `diesel-async` ecosystem

## QuickStart
`Cargo.toml`
```toml
diesel = "2"
diesel-async = "0.5"
xitca-postgres-diesel = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```
`main.rs`
```rust
// diesel is used for dsl query building
use diesel::prelude::*;
// diesel-async provides types and traits interface for interacting with diesel dsl
use diesel_async::{RunQueryDsl, AsyncConnection};
// this crate offers lower level pg connection type
use xitca_postgres_diesel::AsyncPgConnection;

// please reference diesel crate for its macro usage
table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // create an async pg connection with xitca_postgres_diesel
    let mut connection = AsyncPgConnection::establish(&std::env::var("DATABASE_URL")?).await?;

    // use diesel query dsl to construct your query
    let data: Vec<User> = users::table
        .filter(users::id.gt(0))
        .or_filter(users::name.like("%Luke"))
        .select(User::as_select())
        // execute the query via the provided
        // async `diesel_async::RunQueryDsl`
        .load(&mut connection)
        .await?;

    Ok(())
}
```