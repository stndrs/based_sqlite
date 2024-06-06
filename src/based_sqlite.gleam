import based.{
  type DB, type Query, type Returned, type Value, DB, Query, Returned,
}
import gleam/dynamic
import gleam/list
import gleam/option.{Some}
import gleam/result
import sqlight.{type Connection}

pub fn with_connection(
  db_name: String,
  callback: fn(DB(a, Connection)) -> t,
) -> t {
  use conn <- sqlight.with_connection(db_name)

  DB(conn: conn, execute: execute) |> callback
}

fn execute(query: Query(a), conn: Connection) -> Result(Returned(a), Nil) {
  let Query(sql, args, maybe_decoder) = query

  let values = to_sqlite_values(args)

  let execution = case maybe_decoder {
    Some(decoder) -> {
      sqlight.query(sql, on: conn, with: values, expecting: decoder)
      |> result.map(fn(rows) {
        let count = list.length(rows)
        Returned(count, rows)
      })
    }
    _ -> {
      sqlight.query(sql, on: conn, with: values, expecting: dynamic.dynamic)
      |> result.replace(Returned(0, []))
    }
  }

  execution |> result.replace_error(Nil)
}

fn to_sqlite_values(values: List(Value)) -> List(sqlight.Value) {
  list.map(values, fn(value) {
    case value {
      based.String(val) -> sqlight.text(val)
      based.Int(val) -> sqlight.int(val)
      based.Float(val) -> sqlight.float(val)
      based.Bool(val) -> sqlight.bool(val)
      based.Null -> sqlight.null()
    }
  })
}
