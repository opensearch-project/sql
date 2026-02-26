/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;

/**
 * Custom ClickHouse SQL dialect that extends Calcite's ClickHouseSqlDialect to handle
 * OpenSearch-specific function translations. This dialect ensures that Calcite-internal function
 * names are mapped back to their ClickHouse equivalents during RelNode-to-SQL unparsing.
 *
 * <p>Quoting: Uses backtick quoting for identifiers (inherited from parent DEFAULT_CONTEXT).
 *
 * <p>Escaping: String literals use single quotes with backslash escaping per ClickHouse rules.
 * Backslashes are escaped as {@code \\} and single quotes as {@code \'}.
 *
 * <p>Date/time literals: Uses ClickHouse function-style syntax (e.g., {@code toDateTime('...')},
 * {@code toDate('...')}), inherited from the parent ClickHouseSqlDialect.
 *
 * <p>Follows the same singleton pattern as {@code OpenSearchSparkSqlDialect}.
 */
public class OpenSearchClickHouseSqlDialect extends ClickHouseSqlDialect {

  /** Singleton instance of the OpenSearch ClickHouse SQL dialect. */
  public static final OpenSearchClickHouseSqlDialect DEFAULT =
      new OpenSearchClickHouseSqlDialect();

  /**
   * Reverse mapping from Calcite-internal function names to their ClickHouse equivalents. When
   * unparsing a RelNode plan back to ClickHouse SQL, these mappings ensure the output uses
   * ClickHouse-native function names.
   */
  private static final Map<String, String> CALCITE_TO_CLICKHOUSE_MAPPING =
      ImmutableMap.of(
          "COUNT_DISTINCT", "uniqExact",
          "ARRAY_AGG", "groupArray",
          "DATE_TRUNC", "toStartOfInterval");

  private OpenSearchClickHouseSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  /**
   * Quotes a string literal using ClickHouse escaping rules. ClickHouse uses single-quoted string
   * literals with backslash escaping:
   *
   * <ul>
   *   <li>Backslash ({@code \}) is escaped as {@code \\}
   *   <li>Single quote ({@code '}) is escaped as {@code \'}
   * </ul>
   *
   * <p>This differs from the default Calcite behavior which doubles single quotes ({@code ''}).
   *
   * @param buf the buffer to append to
   * @param charsetName the charset name (ignored, ClickHouse does not support charset prefixes)
   * @param val the string value to quote
   */
  @Override
  public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
    buf.append('\'');
    for (int i = 0; i < val.length(); i++) {
      char c = val.charAt(i);
      if (c == '\\') {
        buf.append("\\\\");
      } else if (c == '\'') {
        buf.append("\\'");
      } else {
        buf.append(c);
      }
    }
    buf.append('\'');
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String operatorName = call.getOperator().getName();
    if (CALCITE_TO_CLICKHOUSE_MAPPING.containsKey(operatorName)) {
      unparseClickHouseFunction(writer, call, CALCITE_TO_CLICKHOUSE_MAPPING.get(operatorName));
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Unparses a function call using the ClickHouse-native function name, preserving all operands.
   */
  private void unparseClickHouseFunction(SqlWriter writer, SqlCall call, String functionName) {
    writer.print(functionName);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (int i = 0; i < call.operandCount(); i++) {
      if (i > 0) {
        writer.sep(",");
      }
      call.operand(i).unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }
}
