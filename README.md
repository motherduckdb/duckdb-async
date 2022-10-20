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

# Development Note (Oct 2020)

(This is implementation detail, only relevant if you are involved in development or maintenance of this library)

TypeScript declarations were added to the main DuckDb repository in [This PR](https://github.com/duckdb/duckdb/pull/5025).
Since these declarations will only be available in the `duckdb` npm package (and corresponding binary builds) once
5.2.0 is released, this repository currently contains its own copy of the TypeScript declarations for the underlying
NodeJS API. These are only included as an interim stopgap to enable users to try this library without needing to build DuckDb
from source while waiting for the 5.2.0 release.

For simplicity and clarity for users of this library on compatibility with duckdb releases, versions of this library will
follow the same version numbers as the underlying `duckdb` npm package.
