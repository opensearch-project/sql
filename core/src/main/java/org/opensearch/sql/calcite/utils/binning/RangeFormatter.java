/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.CalcitePlanContext;

/** Utility class for creating formatted range strings. */
public class RangeFormatter {

  /** Creates a formatted range string from start and end values. */
  public static RexNode createRangeString(
      RexNode binValue, RexNode binEnd, CalcitePlanContext context) {
    return createRangeString(binValue, binEnd, null, context);
  }

  /** Creates a formatted range string with optional width formatting. */
  public static RexNode createRangeString(
      RexNode binValue, RexNode binEnd, RexNode width, CalcitePlanContext context) {

    RexNode dash = context.relBuilder.literal(BinConstants.DASH_SEPARATOR);

    RexNode binValueFormatted =
        width != null
            ? createFormattedValue(binValue, width, context)
            : context.relBuilder.cast(binValue, SqlTypeName.VARCHAR);

    RexNode binEndFormatted =
        width != null
            ? createFormattedValue(binEnd, width, context)
            : context.relBuilder.cast(binEnd, SqlTypeName.VARCHAR);

    RexNode firstConcat =
        context.relBuilder.call(SqlStdOperatorTable.CONCAT, binValueFormatted, dash);

    return context.relBuilder.call(SqlStdOperatorTable.CONCAT, firstConcat, binEndFormatted);
  }

  /** Creates a formatted value that shows integers without decimals when appropriate. */
  private static RexNode createFormattedValue(
      RexNode value, RexNode width, CalcitePlanContext context) {

    RexNode isIntegerWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, width, context.relBuilder.literal(1.0));

    RexNode integerValue =
        context.relBuilder.cast(
            context.relBuilder.cast(value, SqlTypeName.INTEGER), SqlTypeName.VARCHAR);

    RexNode decimalValue = context.relBuilder.cast(value, SqlTypeName.VARCHAR);

    return context.relBuilder.call(
        SqlStdOperatorTable.CASE, isIntegerWidth, integerValue, decimalValue);
  }
}
