import type { ColumnRef, SelectStatement } from "./ast";
import { Err, Ok } from "./result";
import type { Result } from "./result";

export type TableDefs = ReturnType<typeof defineTables>;

export function defineTables<
  T extends {
    [Table in keyof T]: Record<string, null | { [FK in keyof T & string]?: keyof T[FK] & string }>;
  },
>(tables: T) {
  return tables;
}

export function checkJoins(ast: SelectStatement, tables: TableDefs): Result<SelectStatement> {
  if (!(ast.from.table.name in tables)) {
    return Err(`Table ${ast.from.table.name} is not allowed`);
  }

  for (const join of ast.joins) {
    if (!(join.table.name in tables)) {
      return Err(`Table ${join.table.name} is not allowed`);
    }
    const joinSettings = tables[join.table.name];
    const allowedColRef: ColumnRef = {
      type: "column_ref",
      name: Object.keys(joinSettings)[0],
    };
    const allowedFkRef: ColumnRef = {
      type: "column_ref",
      table: Object.keys(Object.values(joinSettings)[0]!)[0],
      name: Object.values(Object.values(joinSettings)[0]!)[0]!,
    };

    const joinCondition = join.condition;
    if (joinCondition === null) {
      return Err("Joins without conditions not supported");
    }
    if (joinCondition.type === "join_using") {
      return Err("Join USING not supported");
    }
    if (joinCondition.expr.type !== "where_comparison") {
      return Err("Only single JOIN ON foo = bar supported");
    }
    if (joinCondition.expr.operator !== "=") {
      return Err("Only = supported");
    }
    if (joinCondition.expr.left.type !== "where_value") {
      return Err("Only where_value supported");
    }
    if (joinCondition.expr.left.kind !== "column_ref") {
      return Err("Only column_ref supported supported");
    }
    if (joinCondition.expr.right.type !== "where_value") {
      return Err("Only where_value supported");
    }
    if (joinCondition.expr.right.kind !== "column_ref") {
      return Err("Only column_ref supported supported");
    }
    if (!columnRefEquals(joinCondition.expr.left.ref, allowedColRef)) {
      return Err("Illegal join on left!");
    }
    if (!columnRefEquals(joinCondition.expr.right.ref, allowedFkRef)) {
      return Err("Illegal join on right!");
    }
  }

  return Ok(ast);
}

function columnRefEquals(a: ColumnRef, b: ColumnRef) {
  return a.table === b.table && a.name === b.name;
}
