import { DatabaseSync } from "node:sqlite";

import { ddl } from "../ddl";

let db: DatabaseSync | undefined;

export function setupDb(): DatabaseSync {
  db = new DatabaseSync(":memory:");

  for (const query of ddl) {
    db.exec(query);
  }

  return db;
}

export function teardownDb(): void {
  if (db) {
    db.close();
    db = undefined;
  }
}
