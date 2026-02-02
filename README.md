an async ORM for postgresql built upon [diesel](https://crates.io/crates/diesel)

## Usage
`xitca-postgres-diesel` is an extension crate of `diesel` by offering PostgreSQL connection type enabling async to it's ecosystem

## QuickStart
`Cargo.toml`
```toml
diesel = "2"
xitca-postgres-diesel = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```
`main.rs`
```rust
// diesel is used for dsl query building
use diesel::prelude::*;
// this crate offers lower level pg connection type and execution methods of diesel query builder
use xitca_postgres_diesel::{AsyncPgConnection, RunQueryDsl};

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
    let connection = AsyncPgConnection::establish(&std::env::var("DATABASE_URL")?).await?;

    // use diesel query dsl to construct your query
    let data: Vec<User> = users::table
        .filter(users::id.gt(0))
        .or_filter(users::name.like("%Luke"))
        .select(User::as_select())
        // execute the query via the provided `xitca_postgres_diesel::RunQueryDsl` trait
        .load(&connection)
        .await?;

    Ok(())
}
```