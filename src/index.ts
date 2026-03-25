import { checkFunctions, DEFAULT_DB, type DbType } from "./functions";
import { insertNeededGuardJoins } from "./graph";
import {
  applyGuards,
  resolveGuards,
  type GuardVal,
  type SchemaGuardKeys,
  DEFAULT_LIMIT,
} from "./guard";
import { defineSchema, validateJoins, type Schema } from "./joins";
import { outputSql } from "./output";
import { parseSql } from "./parse";
import { Ok, type Result } from "./result";

export { parseSql, applyGuards as sanitiseSql, outputSql, defineSchema };
export type { DbType } from "./functions";

/*
 * agentSql uses the provided arguments to sanitise and harden a SQL query.
 *
 * ```ts
 * const schema = defineSchema({ tenant: { id: null } });
 * const sanitised = agentSql("SELEcT * FROM tenant", { "tenant.id": 123 }, schema);
 * ```
 *
 * Notes on arguments:
 *
 * # guards
 * Specifies the tenant/ownership locks to enforce.
 * If `guards` is EMPTY, no tenant isolation will be enforced.
 *
 * ```json
 * { "tenant.id": 3, "user.id": 5 }
 * ```
 *
 * will add
 *
 * ```sql
 * WHERE tenant.id = 3 AND user.id = 5
 * ```
 *
 * to the query, and ensure that `tenant` and `user` tables are joined to the query.
 *
 * # schema
 * Defines which tables and joins are permitted.
 * If no schema is provided, no JOINS will be permitted.
 *
 * # autoJoin
 * Whether agent-sql will automatically insert needed JOINs to reach guard tables.
 * Default true
 *
 * # limit
 * What `LIMIT n` value to insert.
 * Default 10000
 *
 * # pretty
 * Whether the pretty-print the SQL output.
 * Default false
 *
 * # throws
 * Whether to throw Exceptions or return a Result.
 * Default true
 *
 * # db
 * The DB being used. Only used to control default function allowlist.
 * Default postgres
 *
 * # allowExtraFunctions
 * List of additional functions to whitelist.
 *
 * Guard keys are type-checked against the schema: only `"table.column"`
 * combinations that actually exist in the schema are accepted.
 *
 * If `throws: true` is passed (the default), it will throw on errors.
 * Otherwise it will return a Result type with an `ok` field as discriminator.
 */
export function agentSql<T extends Schema, S extends SchemaGuardKeys<T>>(
  sql: string,
  guards: Record<S, GuardVal>,
  schema: T,
  opts: {
    autoJoin?: boolean;
    limit?: number;
    pretty?: boolean;
    throws: false;
    db?: DbType;
    allowExtraFunctions?: string[];
  },
): Result<string>;
export function agentSql<T extends Schema, S extends SchemaGuardKeys<T>>(
  sql: string,
  guards: Record<S, GuardVal>,
  schema?: T,
  opts?: {
    autoJoin?: boolean;
    limit?: number;
    pretty?: boolean;
    throws?: true;
    db?: DbType;
    allowExtraFunctions?: string[];
  },
): string;
export function agentSql<T extends Schema, S extends SchemaGuardKeys<T>>(
  sql: string,
  guards: Record<S, GuardVal>,
  schema?: T,
  {
    autoJoin = true,
    limit = DEFAULT_LIMIT,
    pretty = false,
    db = DEFAULT_DB,
    allowExtraFunctions = [],
    throws = true,
  }: {
    autoJoin?: boolean;
    limit?: number;
    pretty?: boolean;
    throws?: boolean;
    db?: DbType;
    allowExtraFunctions?: string[];
  } = {},
): Result<string> | string {
  const res = privateAgentSql(sql, {
    guards,
    schema,
    autoJoin,
    limit,
    pretty,
    db,
    allowExtraFunctions,
  });
  if (throws === true) {
    if (res.ok) {
      return res.data;
    }
    throw res.error;
  }
  return res;
}

/*
 * Core logic of the overall agent-sql process. Not used externally.
 */
function privateAgentSql(
  sql: string,
  {
    guards: guardsRaw,
    schema,
    autoJoin,
    limit,
    pretty,
    db,
    allowExtraFunctions,
  }: {
    guards: Record<string, GuardVal>;
    schema: Schema | undefined;
    autoJoin: boolean;
    limit: number;
    pretty: boolean;
    db: DbType;
    allowExtraFunctions: string[];
  },
): Result<string> {
  const guards = resolveGuards(guardsRaw);
  if (!guards.ok) return guards;
  const ast = parseSql(sql);
  if (!ast.ok) return ast;
  const ast2 = validateJoins(ast.data, schema);
  if (!ast2.ok) return ast2;
  const ast3 = checkFunctions(ast2.data, db, allowExtraFunctions);
  if (!ast3.ok) return ast3;
  const ast4 = insertNeededGuardJoins(ast3.data, schema, guards.data, autoJoin);
  if (!ast4.ok) return ast4;
  const san = applyGuards(ast4.data, guards.data, limit);
  if (!san.ok) return san;
  const res = outputSql(san.data, pretty);
  return Ok(res);
}
