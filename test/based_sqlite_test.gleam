import based.{BasedAdapter, BasedError}
import based_sqlite
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn adapter_register_test() {
  let result = {
    let adapter = based_sqlite.adapter(":memory:")

    use db <- based.register(adapter)

    based.new_query("SELECT 1")
    |> based.execute(db)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}

pub fn execute_error_test() {
  let adapter = based_sqlite.adapter(":memory:")

  let error_adapter =
    BasedAdapter(
      with_connection: adapter.with_connection,
      conf: adapter.conf,
      service: fn(_, _) {
        BasedError(code: "an_error", name: "an_error_name", message: "An error")
        |> Error
      },
    )

  let result = {
    use db <- based.register(error_adapter)

    based.new_query("SELECT 1")
    |> based.execute(db)
    |> should.be_error

    Nil
  }

  result |> should.equal(Nil)
}
