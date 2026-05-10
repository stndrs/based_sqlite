import based
import based/sql
import based/sqlite
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import plume
import youid/uuid

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn connect() -> based.Db(plume.Value, sqlite.Connection) {
  sqlite.config(":memory:")
  |> sqlite.db
}

fn with_connection(
  next: fn(based.Db(plume.Value, sqlite.Connection)) -> t,
) -> Result(t, based.BasedError) {
  sqlite.config(":memory:")
  |> sqlite.with_connection(next)
}

fn setup_users(
  db: based.Db(plume.Value, sqlite.Connection),
) -> based.Db(plume.Value, sqlite.Connection) {
  let create_users_table =
    "CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER,
    score REAL
  )"

  let assert Ok(_) = based.execute(create_users_table, db)

  db
}

fn user_decoder() -> decode.Decoder(#(Int, String)) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)
  decode.success(#(id, name))
}

// ---------------------------------------------------------------------------
// Connection tests
// ---------------------------------------------------------------------------

pub fn with_connection_test() {
  let assert Ok("ok") =
    sqlite.config(":memory:")
    |> sqlite.with_connection(fn(_db) { "ok" })
}

pub fn with_connection_error_test() {
  let assert Error(based.DbError(based.ConnectionError("connection failed"))) =
    sqlite.config("/nonexistent/path/to/nowhere/based.sqlite")
    |> sqlite.with_connection(fn(_db) { "ok" })
}

// ---------------------------------------------------------------------------
// Execute tests
// ---------------------------------------------------------------------------

pub fn execute_create_table_test() {
  let db = connect()

  let assert Ok(_) =
    based.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", db)
}

pub fn execute_returns_affected_count_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Alice')", db)
  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Bob')", db)
  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Charlie')", db)

  let assert Ok(2) =
    based.execute("DELETE FROM users WHERE name IN ('Alice', 'Bob')", db)
}

// ---------------------------------------------------------------------------
// Query tests
// ---------------------------------------------------------------------------

pub fn query_insert_and_select_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([plume.text("Alice")])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> based.query(db)

  assert queried.count == 1
  assert queried.fields == ["id", "name"]
}

pub fn query_with_int_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)", db)
  let assert Ok(_) =
    based.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)", db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE age > ?")
    |> sql.params([plume.int(28)])
    |> based.query(db)

  assert queried.count == 1
}

pub fn query_with_float_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("INSERT INTO users (name, score) VALUES ('Alice', 9.5)", db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE score > ?")
    |> sql.params([plume.float(9.0)])
    |> based.query(db)

  assert queried.count == 1
}

pub fn query_with_bool_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute(
      "CREATE TABLE flags (id INTEGER PRIMARY KEY, active INTEGER NOT NULL)",
      db,
    )
  let assert Ok(_) =
    sql.query("INSERT INTO flags (active) VALUES (?)")
    |> sql.params([plume.bool(True)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id FROM flags WHERE active = ?")
    |> sql.params([plume.bool(True)])
    |> based.query(db)

  assert queried.count == 1
}

pub fn query_with_null_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name, email) VALUES (?, ?)")
    |> sql.params([plume.text("Alice"), plume.null])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE email IS NULL")
    |> based.query(db)

  assert queried.count == 1
}

pub fn query_with_bytea_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)", db)

  let data = <<1, 2, 3, 4, 5>>
  let assert Ok(_) =
    sql.query("INSERT INTO blobs (data) VALUES (?)")
    |> sql.params([plume.bytea(data)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT data FROM blobs")
    |> based.query(db)

  assert queried.count == 1
}

pub fn query_with_text_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([plume.text("Alice")])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([plume.text("Alice")])
    |> based.query(db)

  assert queried.count == 1
}

// ---------------------------------------------------------------------------
// Decode / based.all / based.one tests
// ---------------------------------------------------------------------------

pub fn all_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Alice')", db)
  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Bob')", db)

  let assert Ok(users) =
    sql.query("SELECT id, name FROM users ORDER BY name")
    |> based.all(db, user_decoder())

  assert list.length(users) == 2
  let assert [#(_, "Alice"), #(_, "Bob")] = users
}

pub fn one_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    based.execute("INSERT INTO users (name) VALUES ('Alice')", db)

  let assert Ok(#(_, name)) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([plume.text("Alice")])
    |> based.one(db, user_decoder())

  assert name == "Alice"
}

pub fn one_not_found_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Error(based.NotFound) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([plume.text("nobody")])
    |> based.one(db, user_decoder())
}

// ---------------------------------------------------------------------------
// Batch tests
// ---------------------------------------------------------------------------

pub fn batch_test() {
  use db <- with_connection()

  setup_users(db)

  let queries = [
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([plume.text("Alice")]),
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([plume.text("Bob")]),
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([plume.text("Charlie")]),
  ]

  let assert Ok(results) = based.batch(queries, db)
  assert list.length(results) == 3

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> based.query(db)

  assert queried.count == 3
}

// ---------------------------------------------------------------------------
// Transaction tests
// ---------------------------------------------------------------------------

pub fn transaction_commit_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(Nil) =
    sqlite.transaction(db, fn(tx) {
      let assert Ok(_) =
        sql.query("INSERT INTO users (name) VALUES (?)")
        |> sql.params([plume.text("Alice")])
        |> based.query(tx)
      Ok(Nil)
    })

  // Verify the insert persisted
  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> based.query(db)

  assert queried.count == 1
}

pub fn transaction_rollback_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Error(based.Rollback("oops")) =
    sqlite.transaction(db, fn(tx) {
      let assert Ok(_) =
        sql.query("INSERT INTO users (name) VALUES (?)")
        |> sql.params([plume.text("Alice")])
        |> based.query(tx)
      Error("oops")
    })

  // Verify the insert was rolled back
  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> based.query(db)

  assert queried.count == 0
}

pub fn transaction_return_value_test() {
  use db <- with_connection()

  let assert Ok(42) = sqlite.transaction(db, fn(_tx) { Ok(42) })
}

// ---------------------------------------------------------------------------
// Error handling tests
// ---------------------------------------------------------------------------

pub fn syntax_error_test() {
  let db = connect()

  let assert Error(based.DbError(based.SyntaxError(..))) =
    sql.query("SELECTX * FROM nowhere")
    |> based.query(db)
}

pub fn constraint_error_unique_test() {
  use db <- with_connection()

  setup_users(db)

  let users = sql.table("users")

  let rows = {
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(user) { plume.text(user.0) })
    |> sql.value("email", fn(user) { plume.text(user.1) })
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(rows)
    |> sql.to_query(db.sql)
    |> based.query(db)

  // Inserting a duplicate email should fail
  let assert Error(_) =
    sql.query("INSERT INTO users (name, email) VALUES (?, ?)")
    |> sql.params([plume.text("Bob"), plume.text("alice@example.com")])
    |> based.query(db)
}

pub fn constraint_error_not_null_test() {
  let db = connect() |> setup_users

  // Inserting NULL into a NOT NULL column should fail
  let assert Error(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([plume.null])
    |> based.query(db)
}

pub fn execute_syntax_error_test() {
  let db = connect()

  let assert Error(based.DbError(based.SyntaxError(..))) =
    based.execute("NOT VALID SQL", db)
}

pub fn uuid_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    based.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name BLOB)", db)

  let assert Ok(id) =
    uuid.from_string("550e8400-e29b-41d4-a716-446655440000")
    |> result.map(uuid.to_bit_array)

  let assert Ok(_) =
    sql.query("INSERT INTO items (id, name) VALUES (?, ?)")
    |> sql.params([plume.bytea(id), plume.text("widget")])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM items")
    |> based.query(db)

  assert queried.count == 1

  // Decode the UUID text back
  let id_decoder = {
    use id <- decode.field(0, decode.bit_array)
    decode.success(id)
  }
  let assert Ok(returned) = based.decode(queried, id_decoder)
  let assert [stored_id] = returned.rows
  assert stored_id == id
}

// ---------------------------------------------------------------------------
// Date/Time value tests
// ---------------------------------------------------------------------------

pub fn date_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    based.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, date TEXT)", db)

  let date = calendar.Date(2025, calendar.March, 15)
  let assert Ok(_) =
    sql.query("INSERT INTO events (date) VALUES (?)")
    |> sql.params([plume.date(date)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT date FROM events")
    |> based.query(db)

  assert queried.count == 1
}

pub fn time_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    based.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, time TEXT)", db)

  let time = calendar.TimeOfDay(14, 30, 45, 0)
  let assert Ok(_) =
    sql.query("INSERT INTO logs (time) VALUES (?)")
    |> sql.params([plume.time(time)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT time FROM logs")
    |> based.query(db)

  assert queried.count == 1
}

pub fn datetime_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    based.execute(
      "CREATE TABLE records (id INTEGER PRIMARY KEY, created_at TEXT)",
      db,
    )

  let date = calendar.Date(2025, calendar.June, 1)
  let time = calendar.TimeOfDay(12, 0, 0, 0)
  let assert Ok(_) =
    sql.query("INSERT INTO records (created_at) VALUES (?)")
    |> sql.params([plume.datetime(date, time)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT created_at FROM records")
    |> based.query(db)

  assert queried.count == 1
}

pub fn timestamp_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    based.execute("CREATE TABLE stamps (id INTEGER PRIMARY KEY, ts TEXT)", db)

  let ts = timestamp.from_unix_seconds(1_700_000_000)
  let assert Ok(_) =
    sql.query("INSERT INTO stamps (ts) VALUES (?)")
    |> sql.params([plume.timestamp(ts)])
    |> based.query(db)

  let assert Ok(queried) =
    sql.query("SELECT ts FROM stamps")
    |> based.query(db)

  assert queried.count == 1
}

// pub fn duration_value_test() {
//   use db <- with_connection()
// 
//   let assert Ok(_) =
//     based.execute(
//       "CREATE TABLE durations (id INTEGER PRIMARY KEY, dur INTEGER)",
//       db,
//     )
// 
//   let dur =
//     duration.hours(10)
//     |> duration.add(duration.seconds(3600))
// 
//   let assert Ok(_) =
//     sql.query("INSERT INTO durations (dur) VALUES (?)")
//     |> sql.params([plume.duration(dur)])
//     |> based.query(db)
// 
//   let assert Ok(queried) =
//     sql.query("SELECT dur FROM durations")
//     |> based.one(db, {
//   // decode duration
//     })
// 
//   assert queried == dur
// }

pub fn multiple_params_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name, email, age, score) VALUES (?, ?, ?, ?)")
    |> sql.params([
      plume.text("Alice"),
      plume.text("alice@example.com"),
      plume.int(30),
      plume.float(9.5),
    ])
    |> based.query(db)

  let full_decoder = {
    use name <- decode.field(0, decode.string)
    use email <- decode.field(1, decode.string)
    use age <- decode.field(2, decode.int)
    use score <- decode.field(3, decode.float)
    decode.success(#(name, email, age, score))
  }

  let assert Ok([#("Alice", "alice@example.com", 30, 9.5)]) =
    sql.query("SELECT name, email, age, score FROM users")
    |> based.all(db, full_decoder)
}

pub fn query_builder_integration_test() {
  use db <- with_connection()

  setup_users(db)

  let users = sql.table("users")

  let rows = {
    sql.rows([
      #("Alice", 30),
      #("Bob", 25),
      #("Charlie", 35),
    ])
    |> sql.value("name", fn(user) { plume.text(user.0) })
    |> sql.value("age", fn(user) { plume.int(user.1) })
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(rows)
    |> sql.to_query(db.sql)
    |> based.query(db)

  let query =
    sql.from(users)
    |> sql.select([sql.column("name")])
    |> sql.where([sql.gt(sql.column("age"), plume.int(28), of: sql.val)])
    |> sql.order_by([sql.asc(sql.column("name"))])
    |> sql.to_query(db.sql)

  let name_decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  let assert Ok(names) = based.all(query, db, name_decoder)
  assert names == ["Alice", "Charlie"]
}

pub fn empty_result_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(queried) =
    sql.table("users")
    |> sql.from
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.to_query(db.sql)
    |> based.query(db)

  assert queried.count == 0
  assert queried.rows == []
}

pub fn multiple_operations_test() {
  use db <- with_connection()

  setup_users(db)

  let users = sql.table("users")

  let name_inserter = {
    sql.rows(["Alice", "Bob"])
    |> sql.value("name", plume.text)
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(name_inserter)
    |> sql.to_query(db.sql)
    |> based.query(db)

  let assert Ok(_) =
    sql.update(users, [sql.set("name", plume.text("Robert"), of: sql.val)])
    |> sql.where([sql.column("name") |> sql.eq(plume.text("Bob"), of: sql.val)])
    |> sql.to_query(db.sql)
    |> based.query(db)

  let assert Ok(queried) =
    sql.from(users)
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.order_by([sql.asc(sql.column("name"))])
    |> sql.to_query(db.sql)
    |> based.all(db, user_decoder())

  assert list.length(queried) == 2
  let assert [#(_, "Alice"), #(_, "Robert")] = queried
}
