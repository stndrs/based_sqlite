import based/db
import based/interval
import based/sql
import based/sqlite
import based/uuid
import gleam/dynamic/decode
import gleam/list
import gleam/time/calendar
import gleam/time/timestamp

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn connect() -> db.Db(sql.Value, sqlite.Connection) {
  sqlite.config(":memory:")
  |> sqlite.new
}

fn with_connection(
  next: fn(db.Db(sql.Value, sqlite.Connection)) -> t,
) -> Result(t, db.DbError) {
  sqlite.config(":memory:")
  |> sqlite.with_connection(next)
}

fn setup_users(
  db: db.Db(sql.Value, sqlite.Connection),
) -> db.Db(sql.Value, sqlite.Connection) {
  let assert Ok(_) =
    db.execute(
      "CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER,
        score REAL
      )",
      db,
    )
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
  let assert Error(db.ConnectionError(_)) =
    sqlite.config("/nonexistent/path/to/nowhere/db.sqlite")
    |> sqlite.with_connection(fn(_db) { "ok" })
}

// ---------------------------------------------------------------------------
// Execute tests
// ---------------------------------------------------------------------------

pub fn execute_create_table_test() {
  let db = connect()
  let assert Ok(_) =
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", db)
}

pub fn execute_returns_affected_count_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) = db.execute("INSERT INTO users (name) VALUES ('Alice')", db)
  let assert Ok(_) = db.execute("INSERT INTO users (name) VALUES ('Bob')", db)
  let assert Ok(_) =
    db.execute("INSERT INTO users (name) VALUES ('Charlie')", db)

  let assert Ok(2) =
    db.execute("DELETE FROM users WHERE name IN ('Alice', 'Bob')", db)
}

// ---------------------------------------------------------------------------
// Query tests
// ---------------------------------------------------------------------------

pub fn query_insert_and_select_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([sql.text("Alice")])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> db.query(db)

  assert queried.count == 1
  assert queried.fields == ["id", "name"]
}

pub fn query_with_int_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)", db)
  let assert Ok(_) =
    db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)", db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE age > ?")
    |> sql.params([sql.int(28)])
    |> db.query(db)

  assert queried.count == 1
}

pub fn query_with_float_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    db.execute("INSERT INTO users (name, score) VALUES ('Alice', 9.5)", db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE score > ?")
    |> sql.params([sql.float(9.0)])
    |> db.query(db)

  assert queried.count == 1
}

pub fn query_with_bool_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    db.execute(
      "CREATE TABLE flags (id INTEGER PRIMARY KEY, active INTEGER NOT NULL)",
      db,
    )
  let assert Ok(_) =
    sql.query("INSERT INTO flags (active) VALUES (?)")
    |> sql.params([sql.bool(True)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id FROM flags WHERE active = ?")
    |> sql.params([sql.bool(True)])
    |> db.query(db)

  assert queried.count == 1
}

pub fn query_with_null_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name, email) VALUES (?, ?)")
    |> sql.params([sql.text("Alice"), sql.null])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE email IS NULL")
    |> db.query(db)

  assert queried.count == 1
}

pub fn query_with_bytea_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    db.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)", db)

  let data = <<1, 2, 3, 4, 5>>
  let assert Ok(_) =
    sql.query("INSERT INTO blobs (data) VALUES (?)")
    |> sql.params([sql.bytea(data)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT data FROM blobs")
    |> db.query(db)

  assert queried.count == 1
}

pub fn query_with_text_param_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([sql.text("Alice")])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([sql.text("Alice")])
    |> db.query(db)

  assert queried.count == 1
}

// ---------------------------------------------------------------------------
// Decode / db.all / db.one tests
// ---------------------------------------------------------------------------

pub fn all_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) = db.execute("INSERT INTO users (name) VALUES ('Alice')", db)
  let assert Ok(_) = db.execute("INSERT INTO users (name) VALUES ('Bob')", db)

  let assert Ok(users) =
    sql.query("SELECT id, name FROM users ORDER BY name")
    |> db.all(db, user_decoder())

  assert list.length(users) == 2
  let assert [#(_, "Alice"), #(_, "Bob")] = users
}

pub fn one_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) = db.execute("INSERT INTO users (name) VALUES ('Alice')", db)

  let assert Ok(#(_, name)) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([sql.text("Alice")])
    |> db.one(db, user_decoder())

  assert name == "Alice"
}

pub fn one_not_found_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Error(db.NotFound) =
    sql.query("SELECT id, name FROM users WHERE name = ?")
    |> sql.params([sql.text("nobody")])
    |> db.one(db, user_decoder())
}

// ---------------------------------------------------------------------------
// Batch tests
// ---------------------------------------------------------------------------

pub fn batch_test() {
  use db <- with_connection()

  setup_users(db)

  let queries = [
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([sql.text("Alice")]),
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([sql.text("Bob")]),
    sql.query("INSERT INTO users (name) VALUES (?)")
      |> sql.params([sql.text("Charlie")]),
  ]

  let assert Ok(results) = db.batch(queries, db)
  assert list.length(results) == 3

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> db.query(db)

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
        |> sql.params([sql.text("Alice")])
        |> db.query(tx)
      Ok(Nil)
    })

  // Verify the insert persisted
  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> db.query(db)

  assert queried.count == 1
}

pub fn transaction_rollback_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Error(db.Rollback("oops")) =
    sqlite.transaction(db, fn(tx) {
      let assert Ok(_) =
        sql.query("INSERT INTO users (name) VALUES (?)")
        |> sql.params([sql.text("Alice")])
        |> db.query(tx)
      Error("oops")
    })

  // Verify the insert was rolled back
  let assert Ok(queried) =
    sql.query("SELECT id, name FROM users")
    |> db.query(db)

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

  let assert Error(db.SyntaxError(..)) =
    sql.query("SELECTX * FROM nowhere")
    |> db.query(db)
}

pub fn constraint_error_unique_test() {
  use db <- with_connection()

  setup_users(db)

  let users = sql.table("users")

  let rows = {
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.val("name", fn(user) { sql.text(user.0) })
    |> sql.val("email", fn(user) { sql.text(user.1) })
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(rows)
    |> sql.to_query(db.adapter)
    |> db.query(db)

  // Inserting a duplicate email should fail
  let assert Error(_) =
    sql.query("INSERT INTO users (name, email) VALUES (?, ?)")
    |> sql.params([sql.text("Bob"), sql.text("alice@example.com")])
    |> db.query(db)
}

pub fn constraint_error_not_null_test() {
  let db = connect() |> setup_users

  // Inserting NULL into a NOT NULL column should fail
  let assert Error(_) =
    sql.query("INSERT INTO users (name) VALUES (?)")
    |> sql.params([sql.null])
    |> db.query(db)
}

pub fn execute_syntax_error_test() {
  let db = connect()

  let assert Error(db.SyntaxError(..)) = db.execute("NOT VALID SQL", db)
}

// ---------------------------------------------------------------------------
// UUID value test
// ---------------------------------------------------------------------------

pub fn uuid_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name BLOB)", db)

  let assert Ok(id) = uuid.from_string("550e8400-e29b-41d4-a716-446655440000")

  let assert Ok(_) =
    sql.query("INSERT INTO items (id, name) VALUES (?, ?)")
    |> sql.params([sql.uuid(id), sql.text("widget")])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT id, name FROM items")
    |> db.query(db)

  assert queried.count == 1

  // Decode the UUID text back
  let id_decoder = {
    use id <- decode.field(0, decode.bit_array)
    decode.success(id)
  }
  let assert Ok(returned) = db.decode(queried, id_decoder)
  let assert [stored_id] = returned.rows
  assert stored_id == uuid.to_bit_array(id)
}

// ---------------------------------------------------------------------------
// Date/Time value tests
// ---------------------------------------------------------------------------

pub fn date_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, date TEXT)", db)

  let date = calendar.Date(2025, calendar.March, 15)
  let assert Ok(_) =
    sql.query("INSERT INTO events (date) VALUES (?)")
    |> sql.params([sql.date(date)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT date FROM events")
    |> db.query(db)

  assert queried.count == 1
}

pub fn time_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, time TEXT)", db)

  let time = calendar.TimeOfDay(14, 30, 45, 0)
  let assert Ok(_) =
    sql.query("INSERT INTO logs (time) VALUES (?)")
    |> sql.params([sql.time(time)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT time FROM logs")
    |> db.query(db)

  assert queried.count == 1
}

pub fn datetime_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute(
      "CREATE TABLE records (id INTEGER PRIMARY KEY, created_at TEXT)",
      db,
    )

  let date = calendar.Date(2025, calendar.June, 1)
  let time = calendar.TimeOfDay(12, 0, 0, 0)
  let assert Ok(_) =
    sql.query("INSERT INTO records (created_at) VALUES (?)")
    |> sql.params([sql.datetime(date, time)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT created_at FROM records")
    |> db.query(db)

  assert queried.count == 1
}

pub fn timestamp_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE stamps (id INTEGER PRIMARY KEY, ts TEXT)", db)

  let ts = timestamp.from_unix_seconds(1_700_000_000)
  let assert Ok(_) =
    sql.query("INSERT INTO stamps (ts) VALUES (?)")
    |> sql.params([sql.timestamp(ts)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT ts FROM stamps")
    |> db.query(db)

  assert queried.count == 1
}

// ---------------------------------------------------------------------------
// Timestamptz value test (offset is dropped for SQLite)
// ---------------------------------------------------------------------------

pub fn timestamptz_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE stamps_tz (id INTEGER PRIMARY KEY, ts TEXT)", db)

  let ts = timestamp.from_unix_seconds(1_700_000_000)
  let offset = sql.utc_offset(5)
  let assert Ok(_) =
    sql.query("INSERT INTO stamps_tz (ts) VALUES (?)")
    |> sql.params([sql.timestamptz(ts, offset)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT ts FROM stamps_tz")
    |> db.query(db)

  assert queried.count == 1
}

// ---------------------------------------------------------------------------
// Interval value test (stored as ISO 8601 text)
// ---------------------------------------------------------------------------

pub fn interval_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute("CREATE TABLE durations (id INTEGER PRIMARY KEY, dur TEXT)", db)

  let iv =
    interval.Interval(months: 2, days: 15, seconds: 3600, microseconds: 0)
  let assert Ok(_) =
    sql.query("INSERT INTO durations (dur) VALUES (?)")
    |> sql.params([sql.interval(iv)])
    |> db.query(db)

  let assert Ok(queried) =
    sql.query("SELECT dur FROM durations")
    |> db.query(db)

  assert queried.count == 1
}

// ---------------------------------------------------------------------------
// Array value test (stored as JSON text)
// ---------------------------------------------------------------------------

pub fn array_value_test() {
  use db <- with_connection()

  let assert Ok(_) =
    db.execute(
      "CREATE TABLE tags_table (id INTEGER PRIMARY KEY, tags TEXT)",
      db,
    )

  let assert Ok(_) =
    sql.query("INSERT INTO tags_table (tags) VALUES (?)")
    |> sql.params([sql.array(["foo", "bar", "baz"], sql.text)])
    |> db.query(db)

  let str_decoder = {
    use tags <- decode.field(0, decode.string)
    decode.success(tags)
  }

  let assert Ok(queried) =
    sql.query("SELECT tags FROM tags_table")
    |> db.query(db)

  let assert Ok(returned) = db.decode(queried, str_decoder)
  let assert [tags_json] = returned.rows
  assert tags_json == "[\"foo\",\"bar\",\"baz\"]"
}

pub fn multiple_params_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(_) =
    sql.query("INSERT INTO users (name, email, age, score) VALUES (?, ?, ?, ?)")
    |> sql.params([
      sql.text("Alice"),
      sql.text("alice@example.com"),
      sql.int(30),
      sql.float(9.5),
    ])
    |> db.query(db)

  let full_decoder = {
    use name <- decode.field(0, decode.string)
    use email <- decode.field(1, decode.string)
    use age <- decode.field(2, decode.int)
    use score <- decode.field(3, decode.float)
    decode.success(#(name, email, age, score))
  }

  let assert Ok([#("Alice", "alice@example.com", 30, 9.5)]) =
    sql.query("SELECT name, email, age, score FROM users")
    |> db.all(db, full_decoder)
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
    |> sql.val("name", fn(user) { sql.text(user.0) })
    |> sql.val("age", fn(user) { sql.int(user.1) })
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(rows)
    |> sql.to_query(db.adapter)
    |> db.query(db)

  let query =
    sql.from(users)
    |> sql.select([sql.col("name")])
    |> sql.where([sql.gt(sql.col("age"), sql.int(28), of: sql.value)])
    |> sql.order_by(sql.col("name"), sql.asc)
    |> sql.to_query(db.adapter)

  let name_decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  let assert Ok(names) = db.all(query, db, name_decoder)
  assert names == ["Alice", "Charlie"]
}

pub fn empty_result_test() {
  use db <- with_connection()

  setup_users(db)

  let assert Ok(queried) =
    sql.table("users")
    |> sql.from
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.to_query(db.adapter)
    |> db.query(db)

  assert queried.count == 0
  assert queried.rows == []
}

pub fn multiple_operations_test() {
  use db <- with_connection()

  setup_users(db)

  let users = sql.table("users")

  let name_inserter = {
    sql.rows(["Alice", "Bob"])
    |> sql.val("name", sql.text)
  }

  let assert Ok(_) =
    sql.insert(into: users)
    |> sql.values(name_inserter)
    |> sql.to_query(db.adapter)
    |> db.query(db)

  let assert Ok(_) =
    sql.update(users)
    |> sql.set("name", sql.text("Robert"), of: sql.value)
    |> sql.where([sql.col("name") |> sql.eq(sql.text("Bob"), of: sql.value)])
    |> sql.to_query(db.adapter)
    |> db.query(db)

  let assert Ok(queried) =
    sql.from(users)
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.order_by(sql.col("name"), sql.asc)
    |> sql.to_query(db.adapter)
    |> db.all(db, user_decoder())

  assert list.length(queried) == 2
  let assert [#(_, "Alice"), #(_, "Robert")] = queried
}
