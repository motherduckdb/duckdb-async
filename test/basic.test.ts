import * as duckdb from "../src/duckdb-async";
import { Database } from "../src/duckdb-async";

test("t0 - basic database create", async () => {
  const db = await Database.create(":memory:");
  expect(db).toBeInstanceOf(Database);
});

describe("Async API points", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.create(":memory:");
  });

  test("Database.all -- basic query", async () => {
    const results = await db.all("select 42 as a");
    expect(results.length).toBe(1);
    expect(results).toEqual([{ a: 42 }]);
  });

  test("Database.all -- erroneous query", async () => {
    try {
      const results = await db.all("select * from bogusTable");
    } catch (rawErr) {
      const err = rawErr as duckdb.DuckDbError;
      expect(err.message).toContain(
        "Table with name bogusTable does not exist!"
      );
      expect(err.code).toBe("DUCKDB_NODEJS_ERROR");
    }
  });
});
