/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.calcite.rel.rel2sql;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/** OpenSearch-specific RelToSqlConverter extension for recursive CTE support. */
public class OpenSearchRelToSqlConverter extends RelToSqlConverter {

  public OpenSearchRelToSqlConverter(org.apache.calcite.sql.SqlDialect dialect) {
    super(dialect);
  }

  /** Convert a RelNode to a SqlNode while preserving top-level WITH RECURSIVE clauses. */
  public SqlNode toSqlNode(RelNode rel) {
    Result result = visitRoot(rel);
    SqlNode node = result.node;
    if (node.getKind() == org.apache.calcite.sql.SqlKind.WITH) {
      return node;
    }
    return result.asStatement();
  }

  /** Visits a RepeatUnion; called by {@link #dispatch} via reflection. */
  public Result visit(RepeatUnion e) {
    Result seedResult = visitInput(e, 0);
    Result recursiveResult = visitInput(e, 1);

    SqlNode seedNode = seedResult.asSelect();
    SqlNode recursiveNode = recursiveResult.asSelect();
    SqlOperator operator = e.all ? SqlStdOperatorTable.UNION_ALL : SqlStdOperatorTable.UNION;
    SqlNode unionNode = operator.createCall(SqlImplementor.POS, seedNode, recursiveNode);

    SqlIdentifier relationName = buildRelationName(e.getTransientTable());
    SqlNodeList columnList = buildColumnList(e.getRowType());
    SqlWithItem withItem =
        new SqlWithItem(
            SqlImplementor.POS,
            relationName,
            columnList,
            unionNode,
            SqlLiteral.createBoolean(true, SqlImplementor.POS));
    SqlNodeList withList = new SqlNodeList(ImmutableList.of(withItem), SqlImplementor.POS);

    SqlSelect body = wrapSelect(relationName);
    SqlWith withNode = new SqlWith(SqlImplementor.POS, withList, body);
    return result(withNode, ImmutableList.of(Clause.FROM), e, null);
  }

  /** Visits a TableSpool; called by {@link #dispatch} via reflection. */
  public Result visit(TableSpool e) {
    return visitInput(e, 0);
  }

  private static SqlIdentifier buildRelationName(RelOptTable transientTable) {
    RelOptTable table = Objects.requireNonNull(transientTable, "transientTable");
    List<String> qualifiedName = table.getQualifiedName();
    return new SqlIdentifier(qualifiedName, SqlParserPos.QUOTED_ZERO);
  }

  private static SqlNodeList buildColumnList(RelDataType rowType) {
    List<SqlNode> columns =
        rowType.getFieldList().stream()
            .map(RelDataTypeField::getName)
            .map(name -> new SqlIdentifier(name, SqlParserPos.QUOTED_ZERO))
            .map(SqlNode.class::cast)
            .toList();
    return new SqlNodeList(columns, SqlParserPos.QUOTED_ZERO);
  }
}
