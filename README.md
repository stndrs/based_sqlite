# based_sqlite

[![Package Version](https://img.shields.io/hexpm/v/based_sqlite)](https://hex.pm/packages/based_sqlite)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based_sqlite/)
![LLM Usage](https://img.shields.io/badge/LLM%20Usage-Pure%20Vibes-red)

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

pub fn main() {
  // Open an in-memory database (or pass a file path like "./my_app.db")
  let assert Ok(database) = sqlite.db(":memory:")

  // Create a table
  let assert Ok(_) =
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)", database)

  // Insert a row using the query builder
  let assert Ok(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([sql.text("Alice")])
    |> db.query(database)

  // Read it back
  let assert Ok(users) =
    sql.query("SELECT id, name FROM users")
    |> db.all(database, db.decode2(UserRow, db.int, db.text))
}
```

## Transactions

Use `sqlite.transaction` to run a group of operations atomically.
If the callback returns `Ok`, the transaction is committed; if it returns
`Error`, the transaction is rolled back.

```gleam
let assert Ok(database) = sqlite.db(":memory:")

let assert Ok(_) =
  db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)", database)

let assert Ok(Nil) =
  sqlite.transaction(database, fn(tx) {
    let assert Ok(_) =
      db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1", tx)
    let assert Ok(_) =
      db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2", tx)
    Ok(Nil)
  })
```

Further documentation can be found at <https://hexdocs.pm/based_sqlite>.
