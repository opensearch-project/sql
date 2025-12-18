/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;
import org.opensearch.sql.calcite.OpenSearchSchema;

public class SqlRewriteShuttle extends SqlShuttle {
  @Override
  public SqlNode visit(SqlIdentifier id) {
    // Remove database qualifier, keeping only table name
    if (id.names.size() == 2 && OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME.equals(id.names.get(0))) {
      return new SqlIdentifier(Collections.singletonList(id.names.get(1)), id.getParserPosition());
    }
    return super.visit(id);
  }

  @Override
  public @org.checkerframework.checker.nullness.qual.Nullable SqlNode visit(SqlCall call) {
    if (call.getOperator() instanceof SqlCountAggFunction && call.getOperandList().isEmpty()) {
      // Convert COUNT() to COUNT(*) so that SqlCall.isCountStar() resolves to True
      // This is useful when deriving the return types in SqlCountAggFunction#deriveType
      call =
          new SqlBasicCall(
              SqlStdOperatorTable.COUNT,
              List.of(SqlIdentifier.STAR),
              call.getParserPosition(),
              call.getFunctionQuantifier());
    } else if (call.getKind() == SqlKind.IN || call.getKind() == SqlKind.NOT_IN) {
      // Fix for tuple IN / NOT IN queries: Convert SqlNodeList to ROW SqlCall
      //
      // When RelToSqlConverter converts a tuple expression like (id, name) back to
      // SqlNode, it generates a bare SqlNodeList instead of wrapping it in a ROW
      // operator. This causes validation to fail because:
      // 1. SqlValidator.deriveType() doesn't know how to handle SqlNodeList
      // 2. SqlToRelConverter.visit(SqlNodeList) throws UnsupportedOperationException
      //
      // For example, the query:
      //   WHERE (id, name) NOT IN (SELECT uid, name FROM ...)
      //
      // After Rel-to-SQL conversion becomes:
      //   IN operator with operands: [SqlNodeList[id, name], SqlSelect[...]]
      //
      // But it should be:
      //   IN operator with operands: [ROW(id, name), SqlSelect[...]]
      //
      // This fix wraps the SqlNodeList in a ROW SqlCall before validation,
      // ensuring proper type derivation and subsequent SQL-to-Rel conversion.
      if (!call.getOperandList().isEmpty()
          && call.getOperandList().get(0) instanceof SqlNodeList nodes) {
        call.setOperand(0, SqlStdOperatorTable.ROW.createCall(nodes));
      }
    }
    return super.visit(call);
  }
}
