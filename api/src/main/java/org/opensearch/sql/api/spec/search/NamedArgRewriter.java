/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;

/**
 * Pre-validation rewriter for backward compatibility with non-standard named-argument syntax (e.g.,
 * {@code operator='AND'} instead of {@code operator => 'AND'}). Normalizes relevance function calls
 * into MAP-based form so SQL and PPL paths produce identical query plans for pushdown rules.
 *
 * <p>This rewriter is subject to removal if we adopt standard SQL named-argument syntax.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NamedArgRewriter extends SqlShuttle {

  public static final NamedArgRewriter INSTANCE = new NamedArgRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    return UnifiedFunctionSpec.of(visited.getOperator().getName())
        .filter(UnifiedFunctionSpec.RELEVANCE::contains)
        .map(spec -> (SqlNode) rewriteToMaps(visited, spec.getParamNames()))
        .orElse(visited);
  }

  /**
   * Rewrites each argument into a MAP entry. For match(name, 'John', operator='AND'):
   * <li>Positional arg: name → MAP('field', name)
   * <li>Named arg: operator='AND' → MAP('operator', 'AND')
   */
  private static SqlCall rewriteToMaps(SqlCall call, List<String> paramNames) {
    List<SqlNode> operands = call.getOperandList();
    SqlNode[] maps = new SqlNode[operands.size()];
    for (int i = 0; i < operands.size(); i++) {
      SqlNode op = operands.get(i);
      if (op instanceof SqlCall eq && op.getKind() == SqlKind.EQUALS) {
        SqlNode key = eq.operand(0);
        String name =
            key instanceof SqlIdentifier ident
                ? ident.getSimple()
                : key.toString(); // avoid backtick-decorated keys for reserved words
        maps[i] = toMap(name, eq.operand(1));
      } else {
        if (i >= paramNames.size()) {
          throw new IllegalArgumentException(
              String.format("Invalid arguments for function '%s'", call.getOperator().getName()));
        }
        maps[i] = toMap(paramNames.get(i), op);
      }
    }
    return call.getOperator().createCall(call.getParserPosition(), maps);
  }

  private static SqlNode toMap(String key, SqlNode value) {
    return SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR.createCall(
        SqlParserPos.ZERO, SqlLiteral.createCharString(key, SqlParserPos.ZERO), value);
  }
}
