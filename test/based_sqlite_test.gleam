import based
import based_sqlite
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn with_connection_test() {
  let result = {
    use db <- based.register(based_sqlite.with_connection, ":memory:")

    based.new_query("SELECT 1")
    |> based.exec(db)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}
