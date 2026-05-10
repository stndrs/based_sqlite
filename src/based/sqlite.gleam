//// A SQLite adapter for the `based` database abstraction library.
////
//// This module bridges `based` (the generic DB interface) with `plume`
//// (the SQLite driver), providing a `db` function that returns a
//// fully configured `based.Db` handle for executing queries against a SQLite
//// database.

import based
import based/sql
import gleam/list
import gleam/result
import plume

pub opaque type Connection {
  Connection(conn: plume.Connection)
}

pub opaque type Config {
  Config(db: String)
}

pub fn config(db: String) -> Config {
  Config(db:)
}

/// Creates a fully configured `based.Db` record. This function does not open a
/// connection to the configured sqlite database. Passing the `based.Db` record
/// to a query function (e.g. `based/based.query`) will open the connection,
/// perform the query, and then close the connection.
///
/// To work with a `based.Db` that keeps the sqlite connection open for as long
/// as you need, use the `with_connection` function.
pub fn db(config: Config) -> based.Db(plume.Value, Connection) {
  plume.config(config.db)
  |> plume.new
  |> Connection
  |> based.driver(
    on_query: handle_query,
    on_execute: handle_execute,
    on_batch: handle_batch,
  )
  |> based.new(sql.adapter())
}

/// Connects to the configured sqlite database and remains open until the callback
/// completes.
pub fn with_connection(
  config: Config,
  next: fn(based.Db(plume.Value, Connection)) -> t,
) -> Result(t, based.BasedError) {
  plume.config(config.db)
  |> plume.with_connection(fn(conn) {
    Connection(conn:)
    |> based.driver(
      on_query: handle_query,
      on_execute: handle_execute,
      on_batch: handle_batch,
    )
    |> based.new(sql.adapter())
    |> next
  })
  |> result.map_error(to_db_error)
}

/// Runs a callback inside a SQLite transaction.
///
/// The callback receives a `based.Db` record scoped to the transaction.
/// If the callback returns `Ok`, the transaction is committed.
/// If it returns `Error` or crashes, the transaction is rolled back.
pub fn transaction(
  db: based.Db(plume.Value, Connection),
  next: fn(based.Db(plume.Value, Connection)) -> Result(t, error),
) -> Result(t, based.TransactionError(error)) {
  based.transaction(db, tx_handler, next)
}

// ---------------------------------------------------------------------------
// Transaction handler
// ---------------------------------------------------------------------------

fn tx_handler(
  conn: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, based.TransactionError(error)) {
  plume.transaction(conn.conn, fn(plume_conn) { Connection(plume_conn) |> next })
  |> result.map_error(to_tx_error)
}

// ---------------------------------------------------------------------------
// Driver handlers
// ---------------------------------------------------------------------------

fn handle_query(
  query: sql.Query(plume.Value),
  conn: Connection,
) -> Result(based.Queried, based.BasedError) {
  plume.query(query.sql, query.values, conn.conn)
  |> result.map(to_db_queried)
  |> result.map_error(to_db_error)
}

fn handle_execute(
  sql: String,
  conn: Connection,
) -> Result(Int, based.BasedError) {
  plume.execute(sql, on: conn.conn)
  |> result.map_error(to_db_error)
}

fn handle_batch(
  queries: List(sql.Query(plume.Value)),
  conn: Connection,
) -> Result(List(based.Queried), based.BasedError) {
  use query <- list.try_map(queries)

  plume.query(query.sql, query.values, conn.conn)
  |> result.map(to_db_queried)
  |> result.map_error(to_db_error)
}

// ---------------------------------------------------------------------------
// Queried conversion: plume.Queried -> based/based.Queried
// ---------------------------------------------------------------------------

fn to_db_queried(queried: plume.Queried) -> based.Queried {
  based.Queried(
    count: queried.count,
    fields: queried.fields,
    rows: queried.rows,
  )
}

// ---------------------------------------------------------------------------
// Error conversion: plume.PlumeError -> based/based.BasedError
// ---------------------------------------------------------------------------

fn to_db_error(err: plume.PlumeError) -> based.BasedError {
  case err {
    plume.ConnectionFailed -> based.ConnectionError("connection failed")
    plume.ConnectionUnavailable ->
      based.ConnectionError("connection unavailable")
    plume.DbError(code:, message:, detail:, ..) ->
      classify_db_error(code, message, detail)
  }
  |> based.DbError
}

fn classify_db_error(
  code: plume.Code,
  message: String,
  detail: String,
) -> based.DatabaseError {
  let name = code_to_name(code)
  let full_message = case detail {
    "" -> message
    _ -> message <> ": " <> detail
  }

  case is_constraint_error(code) {
    True -> based.ConstraintError(code: name, name: name, message: full_message)
    False ->
      case is_syntax_error(code) {
        True -> based.SyntaxError(code: name, name: name, message: full_message)
        False ->
          based.DatabaseError(code: name, name: name, message: full_message)
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

fn to_tx_error(
  err: plume.TransactionError(error),
) -> based.TransactionError(error) {
  case err {
    plume.RollbackError(cause:) -> based.Rollback(cause:)
    plume.NotInTransaction -> based.NotInTransaction
    plume.TransactionError(message:) -> based.TransactionError(message:)
  }
}
