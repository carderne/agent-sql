import type { DatabaseSync } from "node:sqlite";

import { expect } from "vite-plus/test";

import { defineSchema, createAgentSql } from "../../../src";
import { SanitiseError } from "../../../src/errors";
import { secret } from "../secret";

const schema = defineSchema({
  organization: { id: null },
  user: { id: null, organization_id: { ft: "organization", fc: "id" } },
  message: { user_id: { ft: "user", fc: "id" } },
});
const agentSql = createAgentSql(schema, { "organization.id": 1 }, { throws: false, db: "sqlite" });

function dataIsSafe(object: unknown): boolean {
  return !JSON.stringify(object).includes(secret);
}

export interface Query {
  name: string;
  sql: string;
  // whether we expect the query to pass sanitisation
  expectPassSan: boolean;
}

export function testOneAttack(query: Query, db: DatabaseSync) {
  const san = agentSql(query.sql);

  if (!san.ok) {
    if (!(san.error instanceof SanitiseError)) {
      throw new Error("SQL parsing or something else failed", { cause: san.error });
    }
    expect(query.expectPassSan).toBe(false);
    return;
  }

  expect(query.expectPassSan).toBe(true);

  const result = db.prepare(san.data).all();
  expect(dataIsSafe(result)).toBe(true);
}
