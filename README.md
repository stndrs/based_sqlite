# based_sqlite

[![Package Version](https://img.shields.io/hexpm/v/based_sqlite)](https://hex.pm/packages/based_sqlite)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based_sqlite/)

## WIP

This package should be used with [`based`](https://github.com/stndrs/based)

```sh
gleam add based_sqlite
```

```gleam
import based
import based_sqlite

const sql = "DELETE FROM users WHERE id=$1;"

pub fn main() {
  use db <- based.register(based_sqlite.adapter(":memory:"))

  based.new_query(sql)
  |> based.with_values([based.int(1)])
  |> based.execute(db)
}
```

Further documentation can be found at <https://hexdocs.pm/based_sqlite>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
