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
 *   Most method implementations are thin wrappers around
 *   a call to the result of `util.promisify(...).bind(...)`.
 *   There's some overhead to constructing these functions on
 *   every method call, but I don't expect it to be a
 *   performance bottleneck.
 *   If this overhead becomes an issue, the internal
 *   definitions of `dbCloseAsync` etc could be moved
 *   out of the method bodies to be standalone functions
 */

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

/*
const dbCloseAsync = util.promisify(
  (db: duckdb.Database, callback: Callback<void>) => db.close(callback)
);
*/

/*
const dbAllAsync = util.promisify(
  (
    db: duckdb.Database,
    sql: string,
    ...args: [...any, Callback<duckdb.TableData>] | []
  ) => db.all(sql, ...args)
) as (
  db: duckdb.Database,
  sql: string,
  ...args: any[]
) => Promise<duckdb.TableData>;
*/

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

const dbCloseAsync = methodPromisify<duckdb.Database, void>(
  duckdb.Database.prototype.close
);
const dbAllAsync = methodPromisify<duckdb.Database, duckdb.TableData>(
  duckdb.Database.prototype.all
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
    return dbCloseAsync(this.db);
  }

  // accessor to get internal duckdb Database object
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
      throw new Error("Database.close: uninitialized database");
    }
    /*
    const dbAllAsync = util
      .promisify(duckdb.Database.prototype.all)
      .bind(this.db) as any;
    */
    /*
    const dbAllAsync = util.promisify(
      duckdb.Database.prototype.all.bind(this.db)
    ) as any;
    */

    return dbAllAsync(this.db, sql, ...args);
  }

  /*
  all(sql: string, ...args: any[]): Promise<duckdb.TableData> {
    return new Promise((resolve, reject) => {
      if (!this.db) {
        reject(new Error("Database.all: uninitialized database"));
      } else {
        this.db.all(
          sql,
          ...args,
          (err: duckdb.DuckDbError | null, res: duckdb.TableData) => {
            if (err) {
              reject(err);
            }
            resolve(res);
          }
        );
      }
    });
  }
  */

  /**
   * Executes the sql query and invokes the callback for each row of result data.
   * Since promises can only resolve once, this retains the callback API of the
   * underlying NodeJS API
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

  exec(sql: string, ...args: any[]): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.db) {
        reject(new Error("Database.exec: uninitialized database"));
      } else {
        this.db.exec(sql, ...args, (err: duckdb.DuckDbError | null) => {
          if (err) {
            reject(err);
          }
          resolve();
        });
      }
    });
  }

  /*
  prepare(sql: string, ...args: [...any, Callback<Statement>] | []): Statement;
  run(sql: string, ...args: [...any, Callback<void>] | []): Statement;

  register(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void;
  unregister(name: string, callback: Callback<any>): void;
  */
}
