/**
 * A wrapper around DuckDb node.js API that mirrors that
 * API but uses Promises instead of callbacks.
 *
 */
import * as duckdb from "duckdb";
import { ColumnInfo, TypeInfo } from "duckdb";
import * as util from "util";

type Callback<T> = (err: duckdb.DuckDbError | null, res: T) => void;

export {
  DuckDbError,
  QueryResult,
  RowData,
  TableData,
  OPEN_CREATE,
  OPEN_FULLMUTEX,
  OPEN_PRIVATECACHE,
  OPEN_READONLY,
  OPEN_READWRITE,
  OPEN_SHAREDCACHE,
} from "duckdb";

/*
 * Implmentation note:
 *   Although the method types exposed to users of this library
 *   are reasonably precise, the unfortunate excessive use of
 *   `any` in this utility function is because writing a precise
 *   type for a generic higher order function like
 *   `util.promisify` is beyond the current capabilities of the
 *   TypeScript type system.
 *   See https://github.com/Microsoft/TypeScript/issues/5453
 *   for detailed discussion.
 */
function methodPromisify<T extends object, R>(
  methodFn: (...args: any[]) => any
): (target: T, ...args: any[]) => Promise<R> {
  return util.promisify((target: T, ...args: any[]): any =>
    methodFn.bind(target)(...args)
  ) as any;
}

const connAllAsync = methodPromisify<duckdb.Connection, duckdb.TableData>(
  duckdb.Connection.prototype.all
);

const connArrowIPCAll = methodPromisify<duckdb.Connection, duckdb.ArrowArray>(
  duckdb.Connection.prototype.arrowIPCAll
);

const connExecAsync = methodPromisify<duckdb.Connection, void>(
  duckdb.Connection.prototype.exec
);

const connPrepareAsync = methodPromisify<duckdb.Connection, duckdb.Statement>(
  duckdb.Connection.prototype.prepare
);

const connRunAsync = methodPromisify<duckdb.Connection, duckdb.Statement>(
  duckdb.Connection.prototype.run
);

const connUnregisterUdfAsync = methodPromisify<duckdb.Connection, void>(
  duckdb.Connection.prototype.unregister_udf
);

const connRegisterBufferAsync = methodPromisify<duckdb.Connection, void>(
  duckdb.Connection.prototype.register_buffer
);

const connUnregisterBufferAsync = methodPromisify<duckdb.Connection, void>(
  duckdb.Connection.prototype.unregister_buffer
);

const connCloseAsync = methodPromisify<duckdb.Connection, void>(
  duckdb.Connection.prototype.close
);

export class Connection {
  private conn: duckdb.Connection | null = null;

  private constructor(
    ddb: duckdb.Database,
    resolve: (c: Connection) => void,
    reject: (reason: any) => void
  ) {
    this.conn = new duckdb.Connection(ddb, (err, res: any) => {
      if (err) {
        this.conn = null;
        reject(err);
      }
      resolve(this);
    });
  }

  /**
   * Static method to create a new Connection object. Provided because constructors can not return Promises,
   * and the DuckDb Node.JS API uses a callback in the Database constructor
   */
  static create(db: Database): Promise<Connection> {
    return new Promise((resolve, reject) => {
      new Connection(db.get_ddb_internal(), resolve, reject);
    });
  }

  async all(sql: string, ...args: any[]): Promise<duckdb.TableData> {
    if (!this.conn) {
      throw new Error("Connection.all: uninitialized connection");
    }
    return connAllAsync(this.conn, sql, ...args);
  }

  async arrowIPCAll(sql: string, ...args: any[]): Promise<duckdb.ArrowArray> {
    if (!this.conn) {
      throw new Error("Connection.arrowIPCAll: uninitialized connection");
    }
    return connArrowIPCAll(this.conn, sql, ...args);
  }

  /**
   * Executes the sql query and invokes the callback for each row of result data.
   * Since promises can only resolve once, this method uses the same callback
   * based API of the underlying DuckDb NodeJS API
   * @param sql query to execute
   * @param args parameters for template query
   * @returns
   */
  each(sql: string, ...args: [...any, Callback<duckdb.RowData>] | []): void {
    if (!this.conn) {
      throw new Error("Connection.each: uninitialized connection");
    }
    this.conn.each(sql, ...args);
  }

  /**
   * Execute one or more SQL statements, without returning results.
   * @param sql queries or statements to executes (semicolon separated)
   * @param args parameters if `sql` is a parameterized template
   * @returns `Promise<void>` that resolves when all statements have been executed.
   */
  async exec(sql: string, ...args: any[]): Promise<void> {
    if (!this.conn) {
      throw new Error("Connection.exec: uninitialized connection");
    }
    return connExecAsync(this.conn, sql, ...args);
  }

  prepareSync(sql: string, ...args: any[]): Statement {
    if (!this.conn) {
      throw new Error("Connection.prepareSync: uninitialized connection");
    }
    const ddbStmt = this.conn.prepare(sql, ...(args as any));
    return Statement.create_internal(ddbStmt);
  }

  async prepare(sql: string, ...args: any[]): Promise<Statement> {
    if (!this.conn) {
      throw new Error("Connection.prepare: uninitialized connection");
    }
    const stmt = await connPrepareAsync(this.conn, sql, ...args);
    return Statement.create_internal(stmt);
  }

  runSync(sql: string, ...args: any[]): Statement {
    if (!this.conn) {
      throw new Error("Connection.runSync: uninitialized connection");
    }
    // We need the 'as any' cast here, because run dynamically checks
    // types of args to determine if a callback function was passed in
    const ddbStmt = this.conn.run(sql, ...(args as any));
    return Statement.create_internal(ddbStmt);
  }

  async run(sql: string, ...args: any[]): Promise<Statement> {
    if (!this.conn) {
      throw new Error("Connection.runSync: uninitialized connection");
    }
    const stmt = await connRunAsync(this.conn, sql, ...args);
    return Statement.create_internal(stmt);
  }

  register_udf(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void {
    if (!this.conn) {
      throw new Error("Connection.register_udf: uninitialized connection");
    }
    this.conn.register_udf(name, return_type, fun);
  }
  async unregister_udf(name: string): Promise<void> {
    if (!this.conn) {
      throw new Error("Connection.unregister_udf: uninitialized connection");
    }
    return connUnregisterUdfAsync(this.conn, name);
  }
  register_bulk(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void {
    if (!this.conn) {
      throw new Error("Connection.register_bulk: uninitialized connection");
    }
    this.conn.register_bulk(name, return_type, fun);
  }

  stream(sql: any, ...args: any[]): duckdb.QueryResult {
    if (!this.conn) {
      throw new Error("Connection.stream: uninitialized connection");
    }
    return this.conn.stream(sql, ...args);
  }

  arrowIPCStream(
    sql: any,
    ...args: any[]
  ): Promise<duckdb.IpcResultStreamIterator> {
    if (!this.conn) {
      throw new Error("Connection.arrowIPCStream: uninitialized connection");
    }
    return this.conn.arrowIPCStream(sql, ...args);
  }

  register_buffer(
    name: string,
    array: duckdb.ArrowIterable,
    force: boolean
  ): Promise<void> {
    if (!this.conn) {
      throw new Error("Connection.register_buffer: uninitialized connection");
    }
    return connRegisterBufferAsync(this.conn, name, array, force);
  }

  unregister_buffer(name: string): Promise<void> {
    if (!this.conn) {
      throw new Error("Connection.unregister_buffer: uninitialized connection");
    }
    return connUnregisterBufferAsync(this.conn, name);
  }

  async close(): Promise<void> {
    if (!this.conn) {
      throw new Error("Connection.close: uninitialized connection");
    }
    await connCloseAsync(this.conn);
    this.conn = null;
    return;
  }
}

const dbCloseAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.close
);
const dbAllAsync = methodPromisify<duckdb.Database, duckdb.TableData>(
  duckdb.Database.prototype.all
);
const dbArrowIPCAll = methodPromisify<duckdb.Database, duckdb.ArrowArray>(
  duckdb.Database.prototype.arrowIPCAll
);

const dbExecAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.exec
);

const dbPrepareAsync = methodPromisify<duckdb.Database, duckdb.Statement>(
  duckdb.Database.prototype.prepare
);

const dbRunAsync = methodPromisify<duckdb.Database, duckdb.Statement>(
  duckdb.Database.prototype.run
);

const dbUnregisterUdfAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.unregister_udf
);

const dbSerializeAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.serialize
);

const dbParallelizeAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.parallelize
);

const dbWaitAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.wait
);

const dbRegisterBufferAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.register_buffer
);

const dbUnregisterBufferAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.unregister_buffer
);

export class Database {
  private db: duckdb.Database | null = null;

  private constructor(
    path: string,
    accessMode: number | Record<string, string>,
    resolve: (db: Database) => void,
    reject: (reason: any) => void
  ) {
    if (typeof accessMode === "number") {
      accessMode = {
        access_mode: accessMode == duckdb.OPEN_READONLY ? "read_only" : "read_write"
      };
    }
    accessMode["duckdb_api"] = "nodejs-async";

    this.db = new duckdb.Database(path, accessMode, (err, res) => {
      if (err) {
        reject(err);
      }
      resolve(this);
    });
  }

  /**
   * Static method to create a new Database object. Provided because constructors can not return Promises,
   * and the DuckDb Node.JS API uses a callback in the Database constructor
   */

  /**
   * Static method to create a new Database object from the specified file. Provided as a static
   * method because some initialization may happen asynchronously.
   * @param path path to database file to open, or ":memory:"
   * @returns a promise that resolves to newly created Database object
   */
  static create(
    path: string,
    accessMode?: number | Record<string, string>
  ): Promise<Database> {
    const trueAccessMode = accessMode ?? duckdb.OPEN_READWRITE; // defaults to OPEN_READWRITE
    return new Promise((resolve, reject) => {
      new Database(path, trueAccessMode, resolve, reject);
    });
  }

  async close(): Promise<void> {
    if (!this.db) {
      throw new Error("Database.close: uninitialized database");
    }
    await dbCloseAsync(this.db);
    this.db = null;
    return;
  }

  // accessor to get internal duckdb Database object -- internal use only
  get_ddb_internal(): duckdb.Database {
    if (!this.db) {
      throw new Error("Database.get_ddb_internal: uninitialized database");
    }
    return this.db;
  }

  connect(): Promise<Connection> {
    return Connection.create(this);
  }

  async all(sql: string, ...args: any[]): Promise<duckdb.TableData> {
    if (!this.db) {
      throw new Error("Database.all: uninitialized database");
    }
    return dbAllAsync(this.db, sql, ...args);
  }

  async arrowIPCAll(sql: string, ...args: any[]): Promise<duckdb.ArrowArray> {
    if (!this.db) {
      throw new Error("Database.arrowIPCAll: uninitialized connection");
    }
    return dbArrowIPCAll(this.db, sql, ...args);
  }

  /**
   * Executes the sql query and invokes the callback for each row of result data.
   * Since promises can only resolve once, this method uses the same callback
   * based API of the underlying DuckDb NodeJS API
   * @param sql query to execute
   * @param args parameters for template query
   * @returns
   */
  each(sql: string, ...args: [...any, Callback<duckdb.RowData>] | []): void {
    if (!this.db) {
      throw new Error("Database.each: uninitialized database");
    }
    this.db.each(sql, ...args);
  }

  /**
   * Execute one or more SQL statements, without returning results.
   * @param sql queries or statements to executes (semicolon separated)
   * @param args parameters if `sql` is a parameterized template
   * @returns `Promise<void>` that resolves when all statements have been executed.
   */
  async exec(sql: string, ...args: any[]): Promise<void> {
    if (!this.db) {
      throw new Error("Database.exec: uninitialized database");
    }
    return dbExecAsync(this.db, sql, ...args);
  }

  prepareSync(sql: string, ...args: any[]): Statement {
    if (!this.db) {
      throw new Error("Database.prepareSync: uninitialized database");
    }
    const ddbStmt = this.db.prepare(sql, ...(args as any));
    return Statement.create_internal(ddbStmt);
  }

  async prepare(sql: string, ...args: any[]): Promise<Statement> {
    if (!this.db) {
      throw new Error("Database.prepare: uninitialized database");
    }
    const stmt = await dbPrepareAsync(this.db, sql, ...args);
    return Statement.create_internal(stmt);
  }

  runSync(sql: string, ...args: any[]): Statement {
    if (!this.db) {
      throw new Error("Database.runSync: uninitialized database");
    }
    // We need the 'as any' cast here, because run dynamically checks
    // types of args to determine if a callback function was passed in
    const ddbStmt = this.db.run(sql, ...(args as any));
    return Statement.create_internal(ddbStmt);
  }

  async run(sql: string, ...args: any[]): Promise<Statement> {
    if (!this.db) {
      throw new Error("Database.runSync: uninitialized database");
    }
    const stmt = await dbRunAsync(this.db, sql, ...args);
    return Statement.create_internal(stmt);
  }

  register_udf(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void {
    if (!this.db) {
      throw new Error("Database.register: uninitialized database");
    }
    this.db.register_udf(name, return_type, fun);
  }
  async unregister_udf(name: string): Promise<void> {
    if (!this.db) {
      throw new Error("Database.unregister: uninitialized database");
    }
    return dbUnregisterUdfAsync(this.db, name);
  }

  stream(sql: any, ...args: any[]): duckdb.QueryResult {
    if (!this.db) {
      throw new Error("Database.stream: uninitialized database");
    }
    return this.db.stream(sql, ...args);
  }

  arrowIPCStream(
    sql: any,
    ...args: any[]
  ): Promise<duckdb.IpcResultStreamIterator> {
    if (!this.db) {
      throw new Error("Database.arrowIPCStream: uninitialized database");
    }
    return this.db.arrowIPCStream(sql, ...args);
  }

  serialize(): Promise<void> {
    if (!this.db) {
      throw new Error("Database.serialize: uninitialized database");
    }
    return dbSerializeAsync(this.db);
  }

  parallelize(): Promise<void> {
    if (!this.db) {
      throw new Error("Database.parallelize: uninitialized database");
    }
    return dbParallelizeAsync(this.db);
  }

  wait(): Promise<void> {
    if (!this.db) {
      throw new Error("Database.wait: uninitialized database");
    }
    return dbWaitAsync(this.db);
  }

  interrupt(): void {
    if (!this.db) {
      throw new Error("Database.interrupt: uninitialized database");
    }
    return this.db.interrupt();
  }

  register_buffer(
    name: string,
    array: duckdb.ArrowIterable,
    force: boolean
  ): Promise<void> {
    if (!this.db) {
      throw new Error("Database.register_buffer: uninitialized database");
    }
    return dbRegisterBufferAsync(this.db, name, array, force);
  }

  unregister_buffer(name: string): Promise<void> {
    if (!this.db) {
      throw new Error("Database.unregister_buffer: uninitialized database");
    }
    return dbUnregisterBufferAsync(this.db, name);
  }

  registerReplacementScan(
    replacementScan: duckdb.ReplacementScanCallback
  ): Promise<void> {
    if (!this.db) {
      throw new Error(
        "Database.registerReplacementScan: uninitialized database"
      );
    }
    return this.db.registerReplacementScan(replacementScan);
  }
}

const stmtRunAsync = methodPromisify<duckdb.Statement, void>(
  duckdb.Statement.prototype.run
);

const stmtFinalizeAsync = methodPromisify<duckdb.Statement, void>(
  duckdb.Statement.prototype.finalize
);

const stmtAllAsync = methodPromisify<duckdb.Statement, duckdb.TableData>(
  duckdb.Statement.prototype.all
);

const stmtArrowIPCAllAsync = methodPromisify<
  duckdb.Statement,
  duckdb.ArrowArray
>(duckdb.Statement.prototype.arrowIPCAll);

export class Statement {
  private stmt: duckdb.Statement;

  /**
   * Construct an async wrapper from a statement
   */
  private constructor(stmt: duckdb.Statement) {
    this.stmt = stmt;
  }

  /**
   * create a Statement object that wraps a duckdb.Statement.
   * This is intended for internal use only, and should not be called directly.
   * Use `Database.prepare()` or `Database.run()` to create Statement objects.
   */
  static create_internal(stmt: duckdb.Statement): Statement {
    return new Statement(stmt);
  }

  async all(...args: any[]): Promise<duckdb.TableData> {
    return stmtAllAsync(this.stmt, ...args);
  }
  async arrowIPCAll(...args: any[]): Promise<duckdb.ArrowArray> {
    return stmtArrowIPCAllAsync(this.stmt, ...args);
  }

  /**
   * Executes the sql query and invokes the callback for each row of result data.
   * Since promises can only resolve once, this method uses the same callback
   * based API of the underlying DuckDb NodeJS API
   * @param args parameters for template query, followed by a NodeJS style
   *             callback function invoked for each result row.
   *
   * @returns
   */
  each(...args: [...any, Callback<duckdb.RowData>] | []): void {
    this.stmt.each(...args);
  }

  /**
   * Call `duckdb.Statement.run` directly without awaiting completion.
   * @param args arguments passed to duckdb.Statement.run()
   * @returns this
   */
  runSync(...args: any[]): Statement {
    this.stmt.run(...(args as any));
    return this;
  }

  async run(...args: any[]): Promise<Statement> {
    await stmtRunAsync(this.stmt, ...args);
    return this;
  }

  async finalize(): Promise<void> {
    return stmtFinalizeAsync(this.stmt);
  }

  columns(): ColumnInfo[] {
    return this.stmt.columns();
  }
}
