//// A SQLite adapter for the `based` database abstraction library.
////
//// This module bridges `based` (the generic DB interface) with `plume`
//// (the SQLite driver), providing a `db` function that returns a
//// fully configured `db.Db` handle for executing queries against a SQLite
//// database.
////
//// ## Example
////
//// ```gleam
//// import based/db
//// import based/sql
//// import based/sqlite
////
//// pub fn main() {
////   let assert Ok(db) = sqlite.db(":memory:")
////
////   let assert Ok(_) =
////     db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", db)
////
////   let assert Ok(_) =
////     sql.query("INSERT INTO users (name) VALUES (?)")
////     |> sql.params([sql.text("Alice")])
////     |> db.query(db)
//// }
//// ```

import based/db
import based/interval
import based/sql
import based/uuid
import gleam/list
import gleam/result
import gleam/string
import plume

pub opaque type Connection {
  Connection(conn: plume.Connection)
}

/// Creates a new SQLite database connection and returns a fully configured
/// `db.Db` handle.
///
/// The `path` argument is either a file path to a SQLite database file
/// or `":memory:"` for an in-memory database.
///
/// Returns `Error(db.ConnectionError(...))` if the connection cannot be opened.
///
/// ## Example
///
/// ```gleam
/// // In-memory database
/// let assert Ok(db) = sqlite.db(":memory:")
///
/// // File-based database
/// let assert Ok(db) = sqlite.db("./my_app.db")
/// ```
pub fn db(path: String) -> Result(db.Db(sql.Value, Connection), db.DbError) {
  let handle_connect = fn() {
    plume.config(path)
    |> plume.open()
    |> result.map(Connection)
    |> result.map_error(to_db_error)
  }

  let handle_disconnect = fn(conn: Connection) {
    plume.close(conn.conn)
    |> result.replace_error(db.ConnectionError("Failed to disconnect"))
  }

  db.DriverBuilder(
    handle_connect:,
    handle_disconnect:,
    handle_query:,
    handle_execute:,
    handle_batch:,
  )
  |> db.build(sql.adapter())
}

/// Runs a callback inside a SQLite transaction.
///
/// The callback receives a `db.Db` handle scoped to the transaction.
/// If the callback returns `Ok`, the transaction is committed.
/// If it returns `Error`, the transaction is rolled back.
///
/// ## Example
///
/// ```gleam
/// let assert Ok(db) = sqlite.db(":memory:")
///
/// sqlite.transaction(db, fn(tx) {
///   let assert Ok(_) =
///     db.execute("INSERT INTO users (name) VALUES ('Bob')", tx)
///   Ok(Nil)
/// })
/// ```
pub fn transaction(
  db: db.Db(sql.Value, Connection),
  next: fn(db.Db(sql.Value, Connection)) -> Result(t, error),
) -> Result(t, db.TransactionError(error)) {
  db.transaction(db, tx_handler, next)
}

// ---------------------------------------------------------------------------
// Transaction handler
// ---------------------------------------------------------------------------

fn tx_handler(
  conn: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, db.TransactionError(error)) {
  plume.transaction(conn.conn, fn(plume_conn) { Connection(plume_conn) |> next })
  |> result.map_error(to_tx_error)
}

// ---------------------------------------------------------------------------
// Driver handlers
// ---------------------------------------------------------------------------

fn handle_query(
  query: sql.Query(sql.Value),
  conn: Connection,
) -> Result(db.Queried, db.DbError) {
  let values = list.map(query.values, to_plume_value)

  plume.query(query.sql, values, conn.conn)
  |> result.map(to_db_queried)
  |> result.map_error(to_db_error)
}

fn handle_execute(sql: String, conn: Connection) -> Result(Int, db.DbError) {
  plume.exec(sql, on: conn.conn)
  |> result.map_error(to_db_error)
}

fn handle_batch(
  queries: List(sql.Query(sql.Value)),
  conn: Connection,
) -> Result(List(db.Queried), db.DbError) {
  list.try_map(queries, fn(query) { handle_query(query, conn) })
}

// ---------------------------------------------------------------------------
// Value conversion: based/sql.Value -> plume.Value
// ---------------------------------------------------------------------------

fn to_plume_value(value: sql.Value) -> plume.Value {
  case value {
    sql.Null -> plume.Null
    sql.Bool(b) -> plume.Bool(b)
    sql.Int(i) -> plume.Int(i)
    sql.Float(f) -> plume.Float(f)
    sql.Text(s) -> plume.Text(s)
    sql.Bytea(b) -> plume.Bytea(b)
    sql.Date(d) -> plume.Date(d)
    sql.Time(t) -> plume.Time(t)
    sql.Datetime(d, t) -> plume.Datetime(d, t)
    sql.Timestamp(ts) -> plume.Timestamp(ts)
    sql.Uuid(u) -> plume.Bytea(uuid.to_bit_array(u))
    sql.Timestamptz(ts, _offset) -> plume.Timestamp(ts)
    sql.Interval(iv) -> plume.Text(interval.to_iso8601_string(iv))
    sql.Array(vals) -> plume.Text(array_to_json(vals))
  }
}

// ---------------------------------------------------------------------------
// Array serialization (simple JSON)
// ---------------------------------------------------------------------------

fn array_to_json(values: List(sql.Value)) -> String {
  let items = list.map(values, value_to_json)
  "[" <> string.join(items, ",") <> "]"
}

fn value_to_json(value: sql.Value) -> String {
  case value {
    sql.Null -> "null"
    sql.Bool(True) -> "true"
    sql.Bool(False) -> "false"
    sql.Int(i) -> string.inspect(i)
    sql.Float(f) -> string.inspect(f)
    sql.Text(s) -> json_escape_string(s)
    sql.Bytea(_) -> json_escape_string("<binary>")
    sql.Uuid(u) -> json_escape_string(uuid.to_string(u))
    sql.Date(d) -> json_escape_string(date_to_string(d))
    sql.Time(t) -> json_escape_string(time_to_string(t))
    sql.Datetime(d, t) ->
      json_escape_string(date_to_string(d) <> " " <> time_to_string(t))
    sql.Timestamp(ts) -> json_escape_string(string.inspect(ts))
    sql.Timestamptz(ts, _) -> json_escape_string(string.inspect(ts))
    sql.Interval(iv) -> json_escape_string(interval.to_iso8601_string(iv))
    sql.Array(vals) -> array_to_json(vals)
  }
}

fn json_escape_string(s: String) -> String {
  "\""
  <> s
  |> string.replace("\\", "\\\\")
  |> string.replace("\"", "\\\"")
  |> string.replace("\n", "\\n")
  |> string.replace("\r", "\\r")
  |> string.replace("\t", "\\t")
  <> "\""
}

// ---------------------------------------------------------------------------
// Date/time formatting helpers (for JSON serialization of arrays)
// ---------------------------------------------------------------------------

fn date_to_string(date: calendar.Date) -> String {
  pad_zero(date.year, 4)
  <> "-"
  <> pad_zero(month_to_int(date.month), 2)
  <> "-"
  <> pad_zero(date.day, 2)
}

fn time_to_string(time: calendar.TimeOfDay) -> String {
  pad_zero(time.hours, 2)
  <> ":"
  <> pad_zero(time.minutes, 2)
  <> ":"
  <> pad_zero(time.seconds, 2)
}

fn pad_zero(value: Int, width: Int) -> String {
  let s = string.inspect(value)
  let pad = width - string.length(s)
  case pad > 0 {
    True -> string.repeat("0", pad) <> s
    False -> s
  }
}

fn month_to_int(month: calendar.Month) -> Int {
  case month {
    calendar.January -> 1
    calendar.February -> 2
    calendar.March -> 3
    calendar.April -> 4
    calendar.May -> 5
    calendar.June -> 6
    calendar.July -> 7
    calendar.August -> 8
    calendar.September -> 9
    calendar.October -> 10
    calendar.November -> 11
    calendar.December -> 12
  }
}

// ---------------------------------------------------------------------------
// Queried conversion: plume.Queried -> based/db.Queried
// ---------------------------------------------------------------------------

fn to_db_queried(queried: plume.Queried) -> db.Queried {
  db.Queried(count: queried.count, fields: queried.fields, rows: queried.rows)
}

// ---------------------------------------------------------------------------
// Error conversion: plume.PlumeError -> based/db.DbError
// ---------------------------------------------------------------------------

fn to_db_error(err: plume.PlumeError) -> db.DbError {
  case err {
    plume.ConnectionFailed -> db.ConnectionError("connection failed")
    plume.ConnectionUnavailable -> db.ConnectionError("connection unavailable")
    plume.PlumeError(message:) -> db.DbError(message:)
    plume.DbError(code:, message:, detail:, ..) ->
      classify_db_error(code, message, detail)
  }
}

fn classify_db_error(
  code: plume.Code,
  message: String,
  detail: String,
) -> db.DbError {
  let name = code_to_name(code)
  let full_message = case detail {
    "" -> message
    _ -> message <> ": " <> detail
  }

  case is_constraint_error(code) {
    True -> db.ConstraintError(code: name, name: name, message: full_message)
    False ->
      case is_syntax_error(code) {
        True -> db.SyntaxError(code: name, name: name, message: full_message)
        False -> db.DatabaseError(code: name, name: name, message: full_message)
      }
  }
}

fn is_constraint_error(code: plume.Code) -> Bool {
  case code {
    plume.Constraint
    | plume.ConstraintCheck
    | plume.ConstraintCommithook
    | plume.ConstraintDatatype
    | plume.ConstraintForeignkey
    | plume.ConstraintFunction
    | plume.ConstraintNotnull
    | plume.ConstraintPinned
    | plume.ConstraintPrimarykey
    | plume.ConstraintRowid
    | plume.ConstraintTrigger
    | plume.ConstraintUnique
    | plume.ConstraintVtab -> True
    _ -> False
  }
}

fn is_syntax_error(code: plume.Code) -> Bool {
  case code {
    plume.GenericError
    | plume.ErrorMissingCollseq
    | plume.ErrorRetry
    | plume.ErrorSnapshot -> True
    _ -> False
  }
}

fn code_to_name(code: plume.Code) -> String {
  case code {
    plume.Abort -> "ABORT"
    plume.Auth -> "AUTH"
    plume.Busy -> "BUSY"
    plume.Cantopen -> "CANTOPEN"
    plume.Constraint -> "CONSTRAINT"
    plume.Corrupt -> "CORRUPT"
    plume.Done -> "DONE"
    plume.Empty -> "EMPTY"
    plume.GenericError -> "ERROR"
    plume.Format -> "FORMAT"
    plume.Full -> "FULL"
    plume.Internal -> "INTERNAL"
    plume.Interrupt -> "INTERRUPT"
    plume.Ioerr -> "IOERR"
    plume.Locked -> "LOCKED"
    plume.Mismatch -> "MISMATCH"
    plume.Misuse -> "MISUSE"
    plume.Nolfs -> "NOLFS"
    plume.Nomem -> "NOMEM"
    plume.Notadb -> "NOTADB"
    plume.Notfound -> "NOTFOUND"
    plume.Notice -> "NOTICE"
    plume.GenericOk -> "OK"
    plume.Perm -> "PERM"
    plume.Protocol -> "PROTOCOL"
    plume.Range -> "RANGE"
    plume.Readonly -> "READONLY"
    plume.Row -> "ROW"
    plume.Schema -> "SCHEMA"
    plume.Toobig -> "TOOBIG"
    plume.Warning -> "WARNING"
    plume.AbortRollback -> "ABORT_ROLLBACK"
    plume.AuthUser -> "AUTH_USER"
    plume.BusyRecovery -> "BUSY_RECOVERY"
    plume.BusySnapshot -> "BUSY_SNAPSHOT"
    plume.BusyTimeout -> "BUSY_TIMEOUT"
    plume.CantopenConvpath -> "CANTOPEN_CONVPATH"
    plume.CantopenDirtywal -> "CANTOPEN_DIRTYWAL"
    plume.CantopenFullpath -> "CANTOPEN_FULLPATH"
    plume.CantopenIsdir -> "CANTOPEN_ISDIR"
    plume.CantopenNotempdir -> "CANTOPEN_NOTEMPDIR"
    plume.CantopenSymlink -> "CANTOPEN_SYMLINK"
    plume.ConstraintCheck -> "CONSTRAINT_CHECK"
    plume.ConstraintCommithook -> "CONSTRAINT_COMMITHOOK"
    plume.ConstraintDatatype -> "CONSTRAINT_DATATYPE"
    plume.ConstraintForeignkey -> "CONSTRAINT_FOREIGNKEY"
    plume.ConstraintFunction -> "CONSTRAINT_FUNCTION"
    plume.ConstraintNotnull -> "CONSTRAINT_NOTNULL"
    plume.ConstraintPinned -> "CONSTRAINT_PINNED"
    plume.ConstraintPrimarykey -> "CONSTRAINT_PRIMARYKEY"
    plume.ConstraintRowid -> "CONSTRAINT_ROWID"
    plume.ConstraintTrigger -> "CONSTRAINT_TRIGGER"
    plume.ConstraintUnique -> "CONSTRAINT_UNIQUE"
    plume.ConstraintVtab -> "CONSTRAINT_VTAB"
    plume.CorruptIndex -> "CORRUPT_INDEX"
    plume.CorruptSequence -> "CORRUPT_SEQUENCE"
    plume.CorruptVtab -> "CORRUPT_VTAB"
    plume.ErrorMissingCollseq -> "ERROR_MISSING_COLLSEQ"
    plume.ErrorRetry -> "ERROR_RETRY"
    plume.ErrorSnapshot -> "ERROR_SNAPSHOT"
    plume.IoerrAccess -> "IOERR_ACCESS"
    plume.IoerrAuth -> "IOERR_AUTH"
    plume.IoerrBeginAtomic -> "IOERR_BEGIN_ATOMIC"
    plume.IoerrBlocked -> "IOERR_BLOCKED"
    plume.IoerrCheckreservedlock -> "IOERR_CHECKRESERVEDLOCK"
    plume.IoerrClose -> "IOERR_CLOSE"
    plume.IoerrCommitAtomic -> "IOERR_COMMIT_ATOMIC"
    plume.IoerrConvpath -> "IOERR_CONVPATH"
    plume.IoerrCorruptfs -> "IOERR_CORRUPTFS"
    plume.IoerrData -> "IOERR_DATA"
    plume.IoerrDelete -> "IOERR_DELETE"
    plume.IoerrDeleteNoent -> "IOERR_DELETE_NOENT"
    plume.IoerrDirClose -> "IOERR_DIR_CLOSE"
    plume.IoerrDirFsync -> "IOERR_DIR_FSYNC"
    plume.IoerrFstat -> "IOERR_FSTAT"
    plume.IoerrFsync -> "IOERR_FSYNC"
    plume.IoerrGettemppath -> "IOERR_GETTEMPPATH"
    plume.IoerrLock -> "IOERR_LOCK"
    plume.IoerrMmap -> "IOERR_MMAP"
    plume.IoerrNomem -> "IOERR_NOMEM"
    plume.IoerrRdlock -> "IOERR_RDLOCK"
    plume.UnexpectedError -> "UNEXPECTED_ERROR"
  }
}

// ---------------------------------------------------------------------------
// Transaction error conversion
// ---------------------------------------------------------------------------

fn to_tx_error(err: plume.TransactionError(error)) -> db.TransactionError(error) {
  case err {
    plume.RollbackError(cause:) -> db.Rollback(cause:)
    plume.NotInTransaction -> db.NotInTransaction
    plume.TransactionError(message:) -> db.TransactionError(message:)
  }
}

// ---------------------------------------------------------------------------
// Re-export calendar for date/time formatting
// ---------------------------------------------------------------------------

import gleam/time/calendar
