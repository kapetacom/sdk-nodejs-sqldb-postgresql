# NodeJS Postgres support for Kapeta

Creates a client using Prisma and connects to a Postgres database.

Is meant to be used with Kapeta and resources defined in a NodeJS Kapeta block.

Uses Prisma to make it simple to work with Postgres from Kapeta - and 
add support for DB migrations.

Also exposes a CLI tool called ```kap-postgres-url``` that can be used
to generate a PG database URL from within a Kapeta block - for a given environment.

To learn more about Kapeta, visit [kapeta.com](https://kapeta.com).

## Usage

This library exposes a class and function that can be used to create a client.

Normal usage is generated using Kapeta - but it can also be used directly.

### `createPostgresDBClient`
Async function that creates a client using Prisma and connects to a Postgres database

### `PostgresDB`
Class that creates a client using Prisma and connects to a Postgres database.
It will auto-initialize once the configuration provider is ready.