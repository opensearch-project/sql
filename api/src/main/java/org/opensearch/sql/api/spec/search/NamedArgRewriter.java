/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
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
   * <li>Named arg: operator='AND' → MAP('operator', 'AND')
   * <li>Positional arg: name → MAP('field', name)
   * <li>ARRAY arg: ARRAY['f1','f2'] → MAP('fields', MAP(CAST('f1' AS VARCHAR), 1, ...))
   */
  private static SqlCall rewriteToMaps(SqlCall call, List<String> paramNames) {
    List<SqlNode> operands = call.getOperandList();
    SqlNode[] maps = new SqlNode[operands.size()];
    for (int i = 0; i < operands.size(); i++) {
      SqlNode op = operands.get(i);
      if (isNamedArg(op)) {
        maps[i] = namedArgToMap((SqlCall) op);
      } else { // Positional arg
        if (i >= paramNames.size()) {
          throw new IllegalArgumentException(
              String.format("Invalid arguments for function '%s'", call.getOperator().getName()));
        } else if (isArrayArg(op)) {
          maps[i] = map(paramNames.get(i), arrayArgToMap((SqlCall) op));
        } else {
          maps[i] = map(paramNames.get(i), op);
        }
      }
    }
    return call.getOperator().createCall(call.getParserPosition(), maps);
  }

  private static boolean isNamedArg(SqlNode node) {
    return node instanceof SqlCall && node.getKind() == SqlKind.EQUALS;
  }

  private static boolean isArrayArg(SqlNode node) {
    return node instanceof SqlCall call && call.getOperator() == ARRAY_VALUE_CONSTRUCTOR;
  }

  private static SqlNode namedArgToMap(SqlCall eq) {
    SqlNode key = eq.operand(0);
    String name =
        key instanceof SqlIdentifier ident
            ? ident.getSimple()
            : key.toString(); // avoid backtick-decorated keys for reserved words
    return map(name, eq.operand(1));
  }

  private static SqlNode arrayArgToMap(SqlCall arrayCall) {
    List<SqlNode> mapArgs = new ArrayList<>();
    for (SqlNode element : arrayCall.getOperandList()) {
      mapArgs.add(cast(element, VARCHAR));
      mapArgs.add(SqlLiteral.createApproxNumeric("1.0", SqlParserPos.ZERO));
    }
    return map(mapArgs);
  }

  private static SqlNode cast(SqlNode node, SqlTypeName type) {
    SqlDataTypeSpec typeSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type, SqlParserPos.ZERO), SqlParserPos.ZERO);
    return CAST.createCall(SqlParserPos.ZERO, node, typeSpec);
  }

  private static SqlNode map(String key, SqlNode value) {
    return MAP_VALUE_CONSTRUCTOR.createCall(
        SqlParserPos.ZERO, SqlLiteral.createCharString(key, SqlParserPos.ZERO), value);
  }

  private static SqlNode map(List<SqlNode> kvPairs) {
    return MAP_VALUE_CONSTRUCTOR.createCall(SqlParserPos.ZERO, kvPairs.toArray(SqlNode[]::new));
  }
}
