/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * SPLIT function implementation that splits strings by delimiter.
 *
 * <p>Usage: split(str, delimiter)
 *
 * <p>Returns an array of strings split on the delimiter.
 *
 * <p>Special behavior:
 *
 * <ul>
 *   <li>Empty delimiter ("") splits into individual characters
 *   <li>If delimiter not found, returns array with original string
 *   <li>Empty string returns empty array
 * </ul>
 *
 * <p>Implementation notes:
 *
 * <ul>
 *   <li>Uses Calcite's SPLIT for non-empty delimiters
 *   <li>Uses custom character splitting for empty delimiter via REGEXP_REPLACE
 * </ul>
 */
public class SplitFunctionImp implements PPLFuncImpTable.FunctionImp {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode... args) {
    RexNode str = args[0];
    RexNode delimiter = args[1];

    // Check if delimiter is empty string
    // If empty, split into individual characters using a workaround
    // If not empty, use Calcite's SPLIT function

    // Create condition: delimiter = ''
    RexNode emptyString = builder.makeLiteral("");
    RexNode isEmptyDelimiter = builder.makeCall(SqlStdOperatorTable.EQUALS, delimiter, emptyString);

    // For empty delimiter: split into characters
    // Pattern: Insert a delimiter between each character using regex
    // 'abcd' -> 'a|b|c|d' -> split on '|'
    RexNode regexPattern = builder.makeLiteral("(?<=.)(?=.)");
    RexNode replacement = builder.makeLiteral("|");

    // Use REGEXP_REPLACE to insert delimiter between characters
    SqlOperator regexpReplace = SqlLibraryOperators.REGEXP_REPLACE_3;
    RexNode withDelimiters = builder.makeCall(regexpReplace, str, regexPattern, replacement);

    // Then split on the inserted delimiter
    RexNode pipeDelimiter = builder.makeLiteral("|");
    RexNode splitChars = builder.makeCall(SqlLibraryOperators.SPLIT, withDelimiters, pipeDelimiter);

    // For non-empty delimiter: use standard SPLIT
    RexNode normalSplit = builder.makeCall(SqlLibraryOperators.SPLIT, str, delimiter);

    // Use CASE to choose between the two approaches
    // CASE WHEN isEmptyDelimiter THEN splitChars ELSE normalSplit END
    return builder.makeCall(SqlStdOperatorTable.CASE, isEmptyDelimiter, splitChars, normalSplit);
  }
}
