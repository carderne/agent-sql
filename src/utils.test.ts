import { expect, test } from "vite-plus/test";

import { unreachable } from "./utils";

test("throws on invalid SQL", () => {
  expect(() => unreachable(1 as never)).toThrow();
});
