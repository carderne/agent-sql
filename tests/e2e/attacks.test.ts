import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vite-plus/test";

import { defineTables, makeFactory } from "../../src";

const DATABASE_URL = "postgres://pg:pw@localhost:5801/db";

const tables = defineTables({
  organization: { id: null },
  user: { id: null, organizationId: { organization: "id" } },
  message: { userId: { user: "id" } },
});
const guardCol = {
  table: "organization",
  col: "id",
};
const factory = makeFactory({ tables, guardCol, throws: false });
const sanitiser = factory(1);

function dataIsSafe(object: unknown): boolean {
  return !JSON.stringify(object).includes("SECRET");
}

interface Query {
  name: string;
  sql: string;
  expectPassSan: boolean;
}

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
      const san = sanitiser(query.sql);

      if (!san.ok) {
        expect(query.expectPassSan).toBe(false);
        return;
      }

      expect(query.expectPassSan).toBe(true);

      const result = await client.query(san.data);
      expect(dataIsSafe(result)).toBe(true);
    });
  }
});
