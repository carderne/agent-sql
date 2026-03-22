import type { DatabaseSync } from "node:sqlite";

import { afterAll, beforeAll, describe, test } from "vite-plus/test";

import { setupDb, teardownDb } from "./db";
import { Query, testOneAttack } from "./harness";

const queries: Query[] = [
  {
    name: "sqlite-master",
    sql: `
    select * from sqlite_master
    `,
    expectPassSan: false,
  },
  {
    name: "sqlite-schema",
    sql: `
    select * from sqlite_schema
    `,
    expectPassSan: false,
  },
  {
    name: "load-extension-select",
    sql: `
    select load_extension('/tmp/evil.so')
    from organization
    `,
    expectPassSan: false,
  },
  {
    name: "load-extension-where",
    sql: `
    select * from organization
    where load_extension('/tmp/evil.so') is not null
    `,
    expectPassSan: false,
  },
  {
    name: "load-extension-with-entrypoint",
    sql: `
    select load_extension('/tmp/evil.so', 'sqlite3_evil_init')
    from organization
    `,
    expectPassSan: false,
  },
  {
    name: "readfile",
    sql: `
    select readfile('/etc/passwd')
    from organization
    `,
    expectPassSan: false,
  },
  {
    name: "writefile-exfil",
    sql: `
    select writefile('/tmp/exfil.txt', secret)
    from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: false,
  },
  {
    name: "hex-obfuscation",
    sql: `
    select hex(message.secret) from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: true,
  },
  {
    name: "unicode-substr-probe",
    sql: `
    select unicode(substr(message.secret, 1, 1)) from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: true,
  },
  {
    name: "replace-transform",
    sql: `
    select replace(message.secret, 'S', 's') from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: true,
  },
  {
    name: "length-probe",
    sql: `
    select length(message.secret) from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: true,
  },
  {
    name: "typeof-fingerprint",
    sql: `
    select typeof(message.secret) from organization
    join "user" on organization.id = "user".organization_id
    join message on message.user_id = "user".id
    `,
    expectPassSan: true,
  },
];

const singleQuery: Query = {
  name: "new-attack",
  sql: `select * from organization`,
  expectPassSan: true,
};

describe("e2e-sqlite", () => {
  let db: DatabaseSync;

  beforeAll(() => {
    db = setupDb();
  });

  afterAll(() => {
    teardownDb();
  });

  for (const query of queries) {
    test(query.name, () => {
      testOneAttack(query, db);
    });
  }

  test("new-attack", () => {
    testOneAttack(singleQuery, db);
  });
});
