import { PGlite } from "@electric-sql/pglite";

import { ddl } from "../ddl";

let db: PGlite | undefined;

export async function setupDb(): Promise<PGlite> {
  db = new PGlite();

  for (const query of ddl) {
    await db.query(query);
  }

  return db;
}

export async function teardownDb(): Promise<void> {
  if (db) {
    await db.close();
    db = undefined;
  }
}
