import * as duckdb from "../src/duckdb-async";
import { Database } from "../src/duckdb-async";
import fs from "fs";

test("t0 - basic database create", async () => {
  const db = await Database.create(":memory:");
  expect(db).toBeInstanceOf(Database);
});

describe("Async API points", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.create(":memory:");
  });

  test("Database.create -- read only flag", async () => {
    try {
      const roDb = await Database.create(":memory:", duckdb.OPEN_READONLY);
    } catch (rawErr) {
      const err = rawErr as duckdb.DuckDbError;
      expect(err.message).toContain(
        "Cannot launch in-memory database in read-only mode!"
      );
    }
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

  test("Database.exec -- multiple statements (and verify results)", async () => {
    var sql = fs.readFileSync("test/support/script.sql", "utf8");
    await db.exec(sql);
    const rows = await db.all(
      "SELECT type, name FROM sqlite_master ORDER BY type, name"
    );
    expect(rows).toEqual([
      { type: "table", name: "grid_key" },
      { type: "table", name: "grid_utfgrid" },
      { type: "table", name: "images" },
      { type: "table", name: "keymap" },
      { type: "table", name: "map" },
      { type: "table", name: "metadata" },
      { type: "view", name: "grid_data" },
      { type: "view", name: "grids" },
      { type: "view", name: "tiles" },
    ]);
  });

  test("basic connect and Connection.all", async () => {
    const minVal = 1,
      maxVal = 10;

    const conn = await db.connect();

    const rows = await conn.all("SELECT * from range(?,?)", minVal, maxVal);
    expect(rows).toEqual([
      { range: 1 },
      { range: 2 },
      { range: 3 },
      { range: 4 },
      { range: 5 },
      { range: 6 },
      { range: 7 },
      { range: 8 },
      { range: 9 },
    ]);
  });

  test("basic statement prepare/run/finalize", async () => {
    const stmt = await db.prepare(
      "CREATE TABLE foo (txt text, num int, flt double, blb blob)"
    );
    await stmt.runSync().finalize();
  });

  test("Statement.all", async () => {
    const minVal = 1,
      maxVal = 10;
    const stmt = await db.prepare("SELECT * from range(?,?)");
    const rows = await stmt.all(minVal, maxVal);
    expect(rows).toEqual([
      { range: 1 },
      { range: 2 },
      { range: 3 },
      { range: 4 },
      { range: 5 },
      { range: 6 },
      { range: 7 },
      { range: 8 },
      { range: 9 },
    ]);
  });

  test("prepareSync", async () => {
    await db
      .prepareSync("CREATE TABLE foo (txt text, num int, flt double, blb blob)")
      .runSync()
      .finalize();
  });

  test("Database.close", async () => {
    await db.close();
  });
});
