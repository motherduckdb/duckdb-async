/**
 * A wrapper around DuckDb node.js API that mirrors that
 * API but uses Promises instead of callbacks.
 *
 */
import * as duckdb from "duckdb";
import * as util from "util";

type Callback<T> = (err: duckdb.DuckDbError | null, res: T) => void;

export { DuckDbError } from "duckdb";

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
}

const dbCloseAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.close
);
const dbAllAsync = methodPromisify<duckdb.Database, duckdb.TableData>(
  duckdb.Database.prototype.all
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

const dbUnregisterAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.unregister
);

export class Database {
  private db: duckdb.Database | null = null;

  private constructor(
    path: string,
    resolve: (db: Database) => void,
    reject: (reason: any) => void
  ) {
    this.db = new duckdb.Database(path, (err, res) => {
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
  static create(path: string): Promise<Database> {
    return new Promise((resolve, reject) => {
      new Database(path, resolve, reject);
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

  register(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void {
    if (!this.db) {
      throw new Error("Database.close: uninitialized database");
    }
    this.db.register(name, return_type, fun);
  }
  async unregister(name: string): Promise<void> {
    if (!this.db) {
      throw new Error("Database.close: uninitialized database");
    }
    return dbUnregisterAsync(this.db, name);
  }
}

const stmtRunAsync = methodPromisify<duckdb.Statement, void>(
  duckdb.Statement.prototype.run
);

const stmtFinalizeAsync = methodPromisify<duckdb.Statement, void>(
  duckdb.Statement.prototype.finalize
);

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

  /*
    all(...args: [...any, Callback<TableData>] | []): void;
  each(...args: [...any, Callback<RowData>] | []): void;  
  */

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
}
