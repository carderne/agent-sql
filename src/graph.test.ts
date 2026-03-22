import { expect, test } from "vite-plus/test";

import { insertNeededGuardJoins } from "./graph";
import type { WhereGuard } from "./guard";
import { defineSchema } from "./joins";
import { outputSql } from "./output";
import { parseSql } from "./parse";

const schema = defineSchema({
  org: { id: null },
  user: { id: null, org_id: { ft: "org", fc: "id" } },
  message: { id: null, user_id: { ft: "user", fc: "id" } },
  key: { id: null, org_id: { ft: "org", fc: "id" } },
});

const guards: WhereGuard[] = [{ table: "org", column: "id", value: 1 }];

test("resolveGuardJoins adds intermediate joins to reach guard table", () => {
  const ast = parseSql("SELECT * FROM message").unwrap();

  const result = insertNeededGuardJoins(ast, schema, guards, true).unwrap();
  const sql = outputSql(result);

  expect(sql).toBe(
    'SELECT "message".* FROM "message" INNER JOIN "user" ON "user"."id" = "message"."user_id" INNER JOIN "org" ON "org"."id" = "user"."org_id"',
  );
});

test("resolveGuardJoins is a no-op when guard table already present", () => {
  const ast = parseSql("SELECT * FROM org").unwrap();

  const result = insertNeededGuardJoins(ast, schema, guards, true).unwrap();
  const sql = outputSql(result);

  expect(sql).toBe('SELECT * FROM "org"');
});

test("resolveGuardJoins adds single join for directly linked table", () => {
  const ast = parseSql("SELECT * FROM user").unwrap();

  const result = insertNeededGuardJoins(ast, schema, guards, true).unwrap();
  const sql = outputSql(result);

  expect(sql).toBe('SELECT "user".* FROM "user" INNER JOIN "org" ON "org"."id" = "user"."org_id"');
});

test("resolveGuardJoins qualifies wildcard per original table when query has existing joins", () => {
  const ast = parseSql("SELECT * FROM message JOIN user ON user.id = message.user_id").unwrap();

  const result = insertNeededGuardJoins(ast, schema, guards, true).unwrap();
  const sql = outputSql(result);

  expect(sql).toBe(
    'SELECT "message".*, "user".* FROM "message" INNER JOIN "user" ON "user"."id" = "message"."user_id" INNER JOIN "org" ON "org"."id" = "user"."org_id"',
  );
});

test("resolveGuardJoins leaves named columns unchanged", () => {
  const ast = parseSql("SELECT message.id FROM message").unwrap();

  const result = insertNeededGuardJoins(ast, schema, guards, true).unwrap();
  const sql = outputSql(result);

  expect(sql).toBe(
    'SELECT "message"."id" FROM "message" INNER JOIN "user" ON "user"."id" = "message"."user_id" INNER JOIN "org" ON "org"."id" = "user"."org_id"',
  );
});

test("resolveGuardJoins errors when no path exists", () => {
  const isolated = defineSchema({
    org: { id: null },
    other: { id: null },
  });
  const ast = parseSql("SELECT * FROM other").unwrap();

  const result = insertNeededGuardJoins(ast, isolated, guards, true);
  expect(result.ok).toBe(false);
});
