import { secret } from "./secret";

export const ddl: string[] = [
  `CREATE TABLE organization (
      id INTEGER PRIMARY KEY
    )`,

  `CREATE TABLE "user" (
      id INTEGER PRIMARY KEY,
      organization_id INTEGER REFERENCES organization(id)
    )`,

  `CREATE TABLE message (
      user_id INTEGER REFERENCES "user"(id),
      secret TEXT
    )`,

  `INSERT INTO organization (id) VALUES (1), (2)`,
  `INSERT INTO "user" (id, organization_id) VALUES (1, 1), (2, 2)`,
  `INSERT INTO message (user_id, secret) VALUES (1, 'hello'), (2, '${secret}')`,
];
