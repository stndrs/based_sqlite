import based.{Query}
import based_sqlite
import gleam/option.{None}
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn with_connection_test() {
  let result = {
    use db <- based_sqlite.with_connection(":memory:")

    Query(sql: "SELECT 1", args: [], decoder: None) |> db.execute(db.conn)
  }

  result |> should.be_ok
}
