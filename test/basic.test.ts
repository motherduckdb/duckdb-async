import * as duckdb from "../src/duckdb-async";
import { Database } from "../src/duckdb-async";

test("t0 - basic database create", async () => {
  const db = await Database.create(":memory:");
  console.log("got async Database!");
});
