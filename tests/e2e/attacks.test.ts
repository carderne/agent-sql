import { Client } from "pg";
import { afterAll, beforeAll, describe, test } from "vite-plus/test";

import { Query, testOneAttack } from "./harness";

const DATABASE_URL = "postgres://pg:pw@localhost:5801/db";

const queries: Query[] = [
  {
    name: "join-on-true",
    sql: `
    select *
    from organization
    right join "message" on true
    `,
    expectPassSan: false,
  },
  {
    name: "select-orgs",
    sql: `
    select *
    from organization
    `,
    expectPassSan: true,
  },
];

const singleQuery: Query = {
  name: "new-attack",
  sql: `select * from organization`,
  expectPassSan: true,
};

describe("e2e", () => {
  let client: Client;

  beforeAll(async () => {
    client = new Client({ connectionString: DATABASE_URL });
    await client.connect();
  });

  afterAll(async () => {
    await client.end();
  });

  for (const query of queries) {
    test(query.name, async () => {
      await testOneAttack(query, client);
    });
  }

  test("new-attack", async () => {
    await testOneAttack(singleQuery, client);
  });
});
