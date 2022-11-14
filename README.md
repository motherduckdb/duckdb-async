# duckdb-async

TypeScript wrappers using Promises for the duckdb Node.JS API

# Overview and Basic Usage

This repository provides an API that wraps the [DuckDb NodeJS API](https://duckdb.org/docs/api/nodejs/overview) using Promises
instead of callbacks.
The library is implemented in TypeScript to provide static type checking for TypeScript developers. It includes the existing `duckdb`
NPM module as a dependency, so it should be possible to write applications in TypeScript using only `duckdb-async` as a direct dependency.

Basic usage is straightforward. For example:

```typescript
import { Database } from "duckdb-async";

async function simpleTest() {
  const db = await Database.create(":memory:");
  const rows = await db.all("select * from range(1,10)");
  console.log(rows);
}

simpleTest();
```

Note that the static method `Database.create(...)` is used in place of `new Database(...)` in the DuckDb NodeJS API
because the underlying NodeJS API uses a callback in the constructor, and it's not possible to have constructors
return promises.

The API should be relatively complete -- there are wrappers for all of the `Connection`, `Database` and `Statement`
classes from the underlying NodeJS API, with methods that return promises instead of taking callbacks.
A notable exception is the `each` methods on these classes. The `each` method invokes a callback multiple times, once
for each row of the result set. Since promises can only be resolved once, it doesn't make sense to convert this
method to a promise-based API, so the `each` method still provides the same callback-based interface as the
original Node.JS API.
