import { checkJoins, defineTables, type TableDefs } from "./graph";
import { outputSql } from "./output";
import { parseSql } from "./parse";
import { Ok, returnOrThrow, type Result } from "./result";
import { sanitiseSql, type GuardCol, type WhereGuard } from "./sanitise";

export { parseSql, sanitiseSql, outputSql, defineTables };

function privateSanitise(
  sql: string,
  { tables, where, throws }: { tables: TableDefs; where: WhereGuard; throws: false },
): Result<string>;
function privateSanitise(
  sql: string,
  { tables, where, throws }: { tables: TableDefs; where: WhereGuard; throws: true },
): string;
function privateSanitise(
  sql: string,
  { tables, where, throws }: { tables: TableDefs; where: WhereGuard; throws: boolean },
): Result<string> | string {
  const ast = parseSql(sql);
  if (!ast.ok) return returnOrThrow(ast, throws);
  const ast2 = checkJoins(ast.data, tables);
  if (!ast2.ok) return returnOrThrow(ast2, throws);
  const san = sanitiseSql(ast2.data, where);
  if (!san.ok) return returnOrThrow(san, throws);
  const res = outputSql(san.data);
  if (throws) return res;
  return Ok(res);
}

export function sanitise(
  sql: string,
  { tables, where }: { tables: TableDefs; where: WhereGuard },
): string {
  return privateSanitise(sql, { tables, where, throws: true });
}

export function safeSanitise(
  sql: string,
  { tables, where }: { tables: TableDefs; where: WhereGuard },
): Result<string> {
  return privateSanitise(sql, { tables, where, throws: false });
}

export function sanitiserFactory(_: {
  tables: TableDefs;
  where: WhereGuard;
  throws: false;
}): (expr: string) => Result<string>;
export function sanitiserFactory(_: {
  tables: TableDefs;
  where: WhereGuard;
  throws?: true;
}): (expr: string) => string;
export function sanitiserFactory({
  tables,
  where,
  throws = true,
}: {
  tables: TableDefs;
  where: WhereGuard;
  throws?: boolean;
}): (expr: string) => Result<string> | string {
  return (expr: string) =>
    throws
      ? privateSanitise(expr, { tables, where, throws })
      : privateSanitise(expr, { tables, where, throws });
}

export function makeFactory(_: {
  tables: TableDefs;
  guardCol: GuardCol;
  throws: false;
}): (guardVal: string | number) => (expr: string) => Result<string>;
export function makeFactory(_: {
  tables: TableDefs;
  guardCol: GuardCol;
  throws?: true;
}): (guardVal: string | number) => (expr: string) => string;
export function makeFactory({
  tables,
  guardCol,
  throws = true,
}: {
  tables: TableDefs;
  guardCol: GuardCol;
  throws?: boolean;
}): (guardVal: string | number) => (expr: string) => Result<string> | string {
  function factory(guardVal: string | number) {
    const where = { ...guardCol, value: guardVal };
    return throws
      ? sanitiserFactory({ tables, where, throws })
      : sanitiserFactory({ tables, where, throws });
  }
  return factory;
}
