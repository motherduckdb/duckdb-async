/**
 * A wrapper around DuckDb node.js API that mirrors that
 * API but uses ES6 Promises instead of callbacks.
 *
 */
import * as duckdb from "duckdb";

export class Database {
  private db: duckdb.Database = undefined as unknown as duckdb.Database;
  private initialized: boolean;
  private initPromise: Promise<Database>;

  private constructor(path: string) {
    this.initialized = false;
    this.initPromise = new Promise<Database>((resolve, reject) => {
      this.db = new duckdb.Database(path, (err, res) => {
        if (err) {
          reject(err);
        }
        this.initialized = true;
        resolve(this);
      });
    });
  }

  /**
   * Static method to create a new Database object. Provided because constructors can not return Promises,
   * and the DuckDb Node.JS API uses a callback in the Database constructor
   */
  static create(path: string): Promise<Database> {
    const db = new Database(path);
    return db.initPromise;
  }
}
