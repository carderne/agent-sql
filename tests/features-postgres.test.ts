import { expect, test } from "vite-plus/test";

import { parseSql } from "../src";
import { outputSql } from "../src/output";

// --- ILIKE / NOT ILIKE ---

test("ILIKE round-trip", () => {
  const sql = "SELECT id FROM users WHERE name ILIKE '%john%'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "users" WHERE "name" ILIKE '%john%'`,
  );
});

test("NOT ILIKE round-trip", () => {
  const sql = "SELECT id FROM users WHERE name NOT ILIKE '%test%'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "users" WHERE "name" NOT ILIKE '%test%'`,
  );
});

test("ILIKE and NOT ILIKE combined", () => {
  const sql = "SELECT id FROM users WHERE name ILIKE '%foo%' AND email NOT ILIKE '%bar%'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "users" WHERE ("name" ILIKE '%foo%' AND "email" NOT ILIKE '%bar%')`,
  );
});

// --- Cast shorthand (::) ---

test("cast shorthand column::type", () => {
  const sql = "SELECT price::integer FROM products";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT CAST("price" AS integer) FROM "products"`);
});

test("cast shorthand chained expr::type::type", () => {
  const sql = "SELECT val::text::varchar FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT CAST(CAST("val" AS text) AS varchar) FROM "t"`,
  );
});

test("cast shorthand in WHERE", () => {
  const sql = "SELECT id FROM t WHERE created_at::date = '2024-01-01'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "t" WHERE CAST("created_at" AS date) = '2024-01-01'`,
  );
});

// --- DISTINCT ON ---

test("DISTINCT ON single column", () => {
  const sql = "SELECT DISTINCT ON (user_id) user_id, name FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT DISTINCT ON ("user_id") "user_id", "name" FROM "t"`,
  );
});

test("DISTINCT ON multiple columns", () => {
  const sql = "SELECT DISTINCT ON (org_id, user_id) id, name FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT DISTINCT ON ("org_id", "user_id") "id", "name" FROM "t"`,
  );
});

test("DISTINCT ON with ORDER BY", () => {
  const sql =
    "SELECT DISTINCT ON (user_id) user_id, created_at FROM events ORDER BY user_id, created_at DESC";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT DISTINCT ON ("user_id") "user_id", "created_at" FROM "events" ORDER BY "user_id", "created_at" DESC`,
  );
});

// --- JSONB operators ---

test("JSONB -> operator", () => {
  const sql = "SELECT data -> 'key' FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT ("data" -> 'key') FROM "t"`);
});

test("JSONB ->> operator", () => {
  const sql = "SELECT data ->> 'name' FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT ("data" ->> 'name') FROM "t"`);
});

test("JSONB #> and #>> operators", () => {
  const sql = "SELECT data #>> '{a,b}' FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT ("data" #>> '{a,b}') FROM "t"`);
});

test("JSONB @> containment in WHERE", () => {
  const sql = "SELECT id FROM t WHERE data @> '{\"key\": 1}'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "t" WHERE ("data" @> '{"key": 1}')`,
  );
});

test("JSONB ? operator in WHERE", () => {
  const sql = "SELECT id FROM t WHERE data ? 'key'";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT "id" FROM "t" WHERE ("data" ? 'key')`);
});

test("JSONB chained -> ->>", () => {
  const sql = "SELECT data -> 'a' ->> 'b' FROM t";
  expect(outputSql(parseSql(sql).unwrap())).toBe(`SELECT (("data" -> 'a') ->> 'b') FROM "t"`);
});

// --- Text search operators ---

test("@@ text search match", () => {
  const sql = "SELECT id FROM documents WHERE to_tsvector(body) @@ to_tsquery('hello')";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "documents" WHERE to_tsvector("body") @@ to_tsquery('hello')`,
  );
});

test("@@ with column reference", () => {
  const sql = "SELECT id FROM t WHERE tsv @@ to_tsquery('search')";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "t" WHERE "tsv" @@ to_tsquery('search')`,
  );
});

// --- pgvector: distance operators ---

test("pgvector <-> L2 distance", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <-> '[1,2,3]') LIMIT 5`,
  );
});

test("pgvector <#> negative inner product", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <#> '[1,2,3]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <#> '[1,2,3]') LIMIT 5`,
  );
});

test("pgvector <=> cosine distance", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <=> '[1,2,3]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <=> '[1,2,3]') LIMIT 5`,
  );
});

test("pgvector <+> L1 distance", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <+> '[1,2,3]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <+> '[1,2,3]') LIMIT 5`,
  );
});

test("pgvector <~> Hamming distance", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <~> '[1,0,1]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <~> '[1,0,1]') LIMIT 5`,
  );
});

test("pgvector <%> Jaccard distance", () => {
  const sql = "SELECT id FROM items ORDER BY embedding <%> '[1,0,1]' LIMIT 5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" ORDER BY ("embedding" <%> '[1,0,1]') LIMIT 5`,
  );
});

test("pgvector distance in WHERE", () => {
  const sql = "SELECT id FROM items WHERE embedding <-> '[1,2,3]' < 0.5";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id" FROM "items" WHERE ("embedding" <-> '[1,2,3]') < 0.5`,
  );
});

test("pgvector distance in SELECT", () => {
  const sql = "SELECT id, embedding <-> '[1,2,3]' AS distance FROM items";
  expect(outputSql(parseSql(sql).unwrap())).toBe(
    `SELECT "id", ("embedding" <-> '[1,2,3]') AS distance FROM "items"`,
  );
});
