import based.{
  type BasedAdapter, type BasedError, type Query, type Value, BasedAdapter,
  BasedError, Query,
}
import gleam/dynamic.{type Dynamic}
import gleam/list
import gleam/result
import sqlight.{type Connection, SqlightError}

/// Returns a `BasedAdapter` that can be passed into `based.register`
pub fn adapter(db_name: String) -> BasedAdapter(String, Connection, t) {
  BasedAdapter(
    with_connection: with_connection,
    conf: db_name,
    service: execute,
  )
}

fn with_connection(db_name: String, callback: fn(Connection) -> t) -> t {
  sqlight.with_connection(db_name, callback)
}

fn execute(query: Query, conn: Connection) -> Result(List(Dynamic), BasedError) {
  let Query(sql, args) = query

  let values = to_sqlite_values(args)
  sqlight.query(sql, on: conn, with: values, expecting: dynamic.dynamic)
  |> result.map_error(fn(err) {
    let SqlightError(code, message, _offset) = err
    let code = dynamic.from(code) |> dynamic.classify

    BasedError(code: code, name: "", message: message)
  })
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
