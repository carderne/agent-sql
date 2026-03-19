import { expect, test } from "vite-plus/test";

import { sanitiserFactory } from "../src";

test("makeSqyrl returns a function that injects a tenant guard into SQL", () => {
  const query = sanitiserFactory({
    tables: { orders: {} },
    where: { table: "orders", col: "tenant_id", value: "t42" },
  });

  expect(query("SELECT id FROM orders WHERE status = 'open'")).toBe(
    `SELECT "id" FROM "orders" WHERE ("orders"."tenant_id" = 't42' AND "status" = 'open')`,
  );
});
