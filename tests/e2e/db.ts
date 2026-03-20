import { PGlite } from "@electric-sql/pglite";

import { secret } from "./secret";

let db: PGlite | undefined;

export async function setupDb(): Promise<PGlite> {
  db = new PGlite();

  await db.query(`
    CREATE TABLE organization (
      id INTEGER PRIMARY KEY
    )
  `);

  await db.query(`
    CREATE TABLE "user" (
      id INTEGER PRIMARY KEY,
      organization_id INTEGER REFERENCES organization(id)
    )
  `);

  await db.query(`
    CREATE TABLE message (
      user_id INTEGER REFERENCES "user"(id),
      secret TEXT
    )
  `);

  await db.query(`INSERT INTO organization (id) VALUES (1), (2)`);
  await db.query(`INSERT INTO "user" (id, organization_id) VALUES (1, 1), (2, 2)`);
  await db.query(`INSERT INTO message (user_id, secret) VALUES (1, 'hello'), (2, '${secret}')`);

  return db;
}

export async function teardownDb(): Promise<void> {
  if (db) {
    await db.close();
    db = undefined;
  }
}
