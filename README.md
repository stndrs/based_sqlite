# based_sqlite

[![Package Version](https://img.shields.io/hexpm/v/based_sqlite)](https://hex.pm/packages/based_sqlite)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based_sqlite/)
![LLM Usage](https://img.shields.io/badge/LLM%20Usage-Human%20Verified-orange)

A [SQLite](https://sqlite.org/) adapter for [`based`](https://github.com/stndrs/based),
powered by [`plume`](https://github.com/stndrs/plume).

```sh
gleam add based_sqlite
```

## Usage

```gleam
import based/db
import based/sql
import based/sqlite
import gleam/dynamic/decode

pub fn main() {
  // Open an in-memory database (or pass a file path like "./my_app.db")
  let assert Ok(database) = sqlite.db(":memory:")

  // Create a table
  let assert Ok(_) =
    db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
      database,
    )

  // Define an inserter for the users table
  let user_inserter = {
    use <- sql.val("name", fn(name: String) { sql.text(name) })
    sql.row()
  }

  let users = sql.table("users")

  // Insert a row using the query builder
  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(user_inserter, ["Alice"])
    |> sql.to_query(database.adapter)
    |> db.query(database)

  // Read it back
  let user_decoder = {
    use id <- decode.field(0, decode.int)
    use name <- decode.field(1, decode.string)
    decode.success(#(id, name))
  }

  let assert Ok(rows) =
    sql.from(users)
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.to_query(database.adapter)
    |> db.all(database, user_decoder)
}
```

## Transactions

Use `sqlite.transaction` to run a group of operations atomically.
If the callback returns `Ok`, the transaction is committed; if it returns
`Error`, the transaction is rolled back.

```gleam
let assert Ok(database) = sqlite.db(":memory:")

let assert Ok(_) =
  db.execute(
    "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
    database,
  )

let accounts = sql.table("accounts")

let assert Ok(Nil) =
  sqlite.transaction(database, fn(tx) {
    let assert Ok(_) =
      sql.update(accounts)
      |> sql.set("balance", sql.int(-100), of: sql.value)
      |> sql.where([sql.eq(sql.col("id"), sql.int(1), of: sql.value)])
      |> sql.to_query(tx.adapter)
      |> db.query(tx)

    let assert Ok(_) =
      sql.update(accounts)
      |> sql.set("balance", sql.int(100), of: sql.value)
      |> sql.where([sql.eq(sql.col("id"), sql.int(2), of: sql.value)])
      |> sql.to_query(tx.adapter)
      |> db.query(tx)

    Ok(Nil)
  })
```

Further documentation can be found at <https://hexdocs.pm/based_sqlite>.
