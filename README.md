# based_sqlite

[![Package Version](https://img.shields.io/hexpm/v/based_sqlite)](https://hex.pm/packages/based_sqlite)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based_sqlite/)

```sh
gleam add based_sqlite
```
```gleam
import based.{Query, exec}
import based_sqlite
import gleam/option.{None}

const sql = "DELETE FROM users WHERE id=$1;"

pub fn main() {
  use db <- based.register(based_sqlite.with_connection, ":memory:")

  Query(sql: sql, args: [based.int(1)], decoder: None) |> exec(db)
}
```

Further documentation can be found at <https://hexdocs.pm/based_sqlite>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
