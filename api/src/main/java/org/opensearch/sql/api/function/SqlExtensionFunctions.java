/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.util.SqlOperatorTables;

/**
 * Extension operator table for non-standard functions in the unified SQL planning path. Chained
 * with {@link org.apache.calcite.sql.fun.SqlStdOperatorTable} via {@link SqlOperatorTables#chain}
 * in {@link org.apache.calcite.tools.FrameworkConfig} — the standard Calcite mechanism for
 * extending SQL with custom functions.
 *
 * <p>These are lightweight logical operators for SQL parsing and validation only. The {@link
 * RelevanceSearchConvertletTable} swaps them with the PPL operators from {@link
 * org.opensearch.sql.expression.function.PPLBuiltinOperators} during SQL-to-RelNode conversion,
 * bypassing the PPL type checker entirely.
 */
public class SqlExtensionFunctions {

  private SqlExtensionFunctions() {}

  // -- Single-field relevance functions: func(MAP['field', col], MAP['query', 'text'], ...) --

  public static final SqlFunction MATCH = relevanceFunction("match", 2);
  public static final SqlFunction MATCH_PHRASE = relevanceFunction("match_phrase", 2);
  public static final SqlFunction MATCH_BOOL_PREFIX = relevanceFunction("match_bool_prefix", 2);
  public static final SqlFunction MATCH_PHRASE_PREFIX = relevanceFunction("match_phrase_prefix", 2);

  // -- Multi-field relevance functions: func(MAP['query', 'text'], ...) --

  public static final SqlFunction MULTI_MATCH = relevanceFunction("multi_match", 1);
  public static final SqlFunction SIMPLE_QUERY_STRING = relevanceFunction("simple_query_string", 1);
  public static final SqlFunction QUERY_STRING = relevanceFunction("query_string", 1);

  /** All extension functions available to the unified SQL planner. */
  public static final SqlOperatorTable OPERATOR_TABLE =
      SqlOperatorTables.of(
          MATCH,
          MATCH_PHRASE,
          MATCH_BOOL_PREFIX,
          MATCH_PHRASE_PREFIX,
          MULTI_MATCH,
          SIMPLE_QUERY_STRING,
          QUERY_STRING);

  /**
   * Creates a relevance search function that accepts MAP-encoded arguments. Validates only the
   * minimum operand count — actual type checking is deferred to pushdown.
   */
  private static SqlFunction relevanceFunction(String name, int minArgs) {
    return new SqlFunction(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.variadic(SqlOperandCountRanges.from(minArgs)),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
