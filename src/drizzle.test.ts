import { pgTable, text } from "drizzle-orm/pg-core";
import { expect, test } from "vite-plus/test";

import { defineSchemaFromDrizzle } from "./drizzle.js";

const user = pgTable("user", {
  id: text("id"),
  email: text("email"),
});

const organization = pgTable("organization", {
  id: text("id"),
  name: text("name"),
});

const member = pgTable("member", {
  id: text("id"),
  organizationId: text().references(() => organization.id),
  userId: text().references(() => user.id),
});

const chat = pgTable("chat", {
  id: text("chat"),
  memberId: text("member_id").references(() => member.id),
});

const message = pgTable("message", {
  id: text("id"),
  chatId: text("chat_id").references(() => chat.id),
  content: text("content"),
});

const drizzleSchema = { user, organization, member, chat, message };

test("defineSchemaFromDrizzle converts drizzle schema to agent-sql schema", () => {
  const schema = defineSchemaFromDrizzle(drizzleSchema);

  expect(schema).toEqual({
    user: {
      id: null,
      email: null,
    },
    organization: {
      id: null,
      name: null,
    },
    member: {
      id: null,
      organization_id: { ft: "organization", fc: "id" },
      user_id: { ft: "user", fc: "id" },
    },
    chat: {
      chat: null,
      member_id: { ft: "member", fc: "id" },
    },
    message: {
      id: null,
      chat_id: { ft: "chat", fc: "chat" },
      content: null,
    },
  });
});

test("defineSchemaFromDrizzle excludes excluded tables", () => {
  const schema = defineSchemaFromDrizzle(drizzleSchema, { exclude: ["organization", "user"] });

  expect(schema).toEqual({
    member: {
      id: null,
    },
    chat: {
      chat: null,
      member_id: { ft: "member", fc: "id" },
    },
    message: {
      id: null,
      chat_id: { ft: "chat", fc: "chat" },
      content: null,
    },
  });
});
