# duckdb-async

Promise wrappers for the duckdb Node.JS API

# Overview and Basic Usage

This repository provides an API that wraps the [DuckDb NodeJS API](https://duckdb.org/docs/api/nodejs/overview) using Promises
instead of callbacks.
The library is implemented in TypeScript, and includes the existing `duckdb`
NPM module as a dependency, so it should be possible to write applications
in TypeScript using only `duckdb-async` as a direct dependency.

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

# Installation and Versioning

**Note (20Oct2022): As of right now, this library requires a local build of duckdb from source**

This library depends on the TypeScript bindings that have been recently added to DuckDb.
These will be available with the npm duckdb package from version 0.5.2 when it is released.

In the meantime, this npm package has a dependency on "duckdb" of the form:

```json
{
  "duckdb": "../public-duckdb/tools/nodejs"
}
```

which expects a local build of the `duckdb` source tree (from the [main DuckDb github repository](https://github.com/duckdb/duckdb)) in `../public-duckdb`. Note that this must also include a build of the NodeJS bindings for DuckDb.

This library will be updated to change the dependency to the standard duckdb on NPM after duckdb `5.2.0` is released and published to npm.
