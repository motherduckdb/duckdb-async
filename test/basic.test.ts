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

  test("Database.create -- with record arguments", async () => {
    const rwOptsDb = await Database.create(":memory:", {
      access_mode: "READ_WRITE",
      max_memory: "512MB",
      threads: "4",
    });
    const user_agent = await rwOptsDb.all("PRAGMA user_agent");
    expect(user_agent[0]["user_agent"]).toMatch(/duckdb\/[^ ]* nodejs-async/);
  });

  test("Database.create -- explicit numeric read/write flag", async () => {
    const rwDb = await Database.create(":memory:", duckdb.OPEN_READWRITE);
    await rwDb.exec("CREATE TABLE foo (txt text, num int, flt double, blb blob)");
    const empty_result = await rwDb.all("SELECT * FROM foo");
    expect(empty_result.length).toBe(0);
    const user_agent = await rwDb.all("PRAGMA user_agent");
    expect(user_agent[0]["user_agent"]).toMatch(/duckdb\/[^ ]* nodejs-async/);
  });

  test("Database.create -- user agent", async () => {
    const rwDb = await Database.create(":memory:");
    const user_agent = await rwDb.all("PRAGMA user_agent");
    expect(user_agent[0]["user_agent"]).toMatch(/duckdb\/[^ ]* nodejs-async/);
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
      { range: 1n },
      { range: 2n },
      { range: 3n },
      { range: 4n },
      { range: 5n },
      { range: 6n },
      { range: 7n },
      { range: 8n },
      { range: 9n },
    ]);
  });

  test("basic connect and Connection.close", async () => {
    const minVal = 1,
      maxVal = 10;

    const conn = await db.connect();

    const rows = await conn.all("SELECT * from range(?,?)", minVal, maxVal);
    expect(rows).toEqual([
      { range: 1n },
      { range: 2n },
      { range: 3n },
      { range: 4n },
      { range: 5n },
      { range: 6n },
      { range: 7n },
      { range: 8n },
      { range: 9n },
    ]);

    await conn.close();

    try {
      const nextRows = await conn.all(
        "SELECT * from range(?,?)",
        minVal,
        maxVal
      );
    } catch (rawErr) {
      const err = rawErr as duckdb.DuckDbError;
      expect(err.message).toContain("uninitialized connection");
    }
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
      { range: 1n },
      { range: 2n },
      { range: 3n },
      { range: 4n },
      { range: 5n },
      { range: 6n },
      { range: 7n },
      { range: 8n },
      { range: 9n },
    ]);
  });

  test("Statement.columns", async () => {
    const stmt = await db.prepare(
      "SELECT * EXCLUDE(medium_enum, large_enum) FROM test_all_types()"
    );
    const cols = stmt.columns();
    expect(cols).toMatchSnapshot();
  });

  test("prepareSync", async () => {
    await db
      .prepareSync("CREATE TABLE foo (txt text, num int, flt double, blb blob)")
      .runSync()
      .finalize();
  });

  test("ternary int udf", async () => {
    await db.register_udf(
      "udf",
      "integer",
      (x: number, y: number, z: number) => x + y + z
    );
    const rows = await db.all("select udf(21, 20, 1) v");
    expect(rows).toEqual([{ v: 42 }]);
    await db.unregister_udf("udf");
  });

  test("basic stream test", async () => {
    const total = 1000;

    let retrieved = 0;
    const conn = await db.connect();
    const stream = conn.stream("SELECT * FROM range(0, ?)", total);
    for await (const row of stream) {
      retrieved++;
    }
    expect(total).toEqual(retrieved);
  });

  test("arrowIPCAll", async () => {
    const range_size = 100;
    const query = `SELECT * FROM range(0,${range_size}) tbl(i)`;

    try {
      await db.all("INSTALL arrow");
      await db.all("LOAD arrow");
      const result = await db.arrowIPCAll(query);
      expect(result.length).toBe(3);

      const conn = await db.connect();
      const cResult = await conn.arrowIPCAll(query);
      expect(cResult.length).toBe(3);
    } catch (err) {
      console.log("caught error: ", err);
      //expect((err as Error).message).toMatchInlineSnapshot();
    }
  });

  test("Database.close", async () => {
    await db.close();
  });
});
