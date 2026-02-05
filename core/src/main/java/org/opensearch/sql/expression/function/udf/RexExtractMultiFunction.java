/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.expression.parse.RegexCommonUtils;

/** Custom REX_EXTRACT_MULTI function for extracting multiple regex matches. */
public final class RexExtractMultiFunction extends ImplementorUDF {

  public RexExtractMultiFunction() {
    super(new RexExtractMultiImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return call -> {
      var elementType = call.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 2000);
      return call.getTypeFactory()
          .createArrayType(call.getTypeFactory().createTypeWithNullability(elementType, true), -1);
    };
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // Support both (field, pattern, groupIndex, maxMatch) and (field, pattern, groupName, maxMatch)
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            PPLOperandTypes.STRING_STRING_INTEGER_INTEGER
                .getInnerTypeChecker()
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.CHARACTER,
                        SqlTypeFamily.CHARACTER,
                        SqlTypeFamily.CHARACTER,
                        SqlTypeFamily.INTEGER)));
  }

  private static class RexExtractMultiImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression pattern = translatedOperands.get(1);
      Expression groupIndexOrName = translatedOperands.get(2);
      Expression maxMatch = translatedOperands.get(3);

      return Expressions.call(
          RexExtractMultiFunction.class,
          "extractMultipleGroups",
          field,
          pattern,
          groupIndexOrName,
          maxMatch);
    }
  }

  /**
   * Extract multiple regex groups by index (1-based).
   *
   * @param text The input text to extract from
   * @param pattern The regex pattern
   * @param groupIndex The 1-based group index to extract
   * @param maxMatch Maximum number of matches to return (0 = unlimited)
   * @return List of extracted values or null if no matches found
   */
  public static List<String> extractMultipleGroups(
      String text, String pattern, int groupIndex, int maxMatch) {
    if (text == null || pattern == null) {
      return null;
    }

    return executeMultipleExtractions(
        text,
        pattern,
        maxMatch,
        matcher -> {
          if (groupIndex > 0 && groupIndex <= matcher.groupCount()) {
            return matcher.group(groupIndex);
          }
          return null;
        });
  }

  /**
   * Extract multiple occurrences of a named capture group from text. This method avoids the index
   * shifting issue that occurs with nested unnamed groups.
   *
   * @param text The input text to extract from
   * @param pattern The regex pattern with named capture groups
   * @param groupName The name of the capture group to extract
   * @param maxMatch Maximum number of matches to return (0 = unlimited)
   * @return List of extracted values or null if no matches found
   */
  public static List<String> extractMultipleGroups(
      String text, String pattern, String groupName, int maxMatch) {
    if (text == null || pattern == null || groupName == null) {
      return null;
    }

    return executeMultipleExtractions(
        text,
        pattern,
        maxMatch,
        matcher -> {
          try {
            return matcher.group(groupName);
          } catch (IllegalArgumentException e) {
            // Group name doesn't exist in the pattern, stop processing
            return null;
          }
        });
  }

  /**
   * Common extraction logic for multiple matches to avoid code duplication.
   *
   * @param text The input text
   * @param pattern The regex pattern
   * @param maxMatch Maximum matches (0 = unlimited)
   * @param extractor Function to extract the value from the matcher
   * @return List of extracted values or null if no matches found
   */
  private static List<String> executeMultipleExtractions(
      String text,
      String pattern,
      int maxMatch,
      java.util.function.Function<Matcher, String> extractor) {
    try {
      Pattern compiledPattern = RegexCommonUtils.getCompiledPattern(pattern);
      Matcher matcher = compiledPattern.matcher(text);
      List<String> matches = new ArrayList<>();

      int matchCount = 0;
      while (matcher.find() && (maxMatch == 0 || matchCount < maxMatch)) {
        String match = extractor.apply(matcher);
        if (match != null) {
          matches.add(match);
          matchCount++;
        } else {
          // If extractor returns null, it might indicate an error (like invalid group name)
          // Stop processing to avoid infinite loop
          break;
        }
      }

      return matches.isEmpty() ? null : matches;
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Error in 'rex' command: Encountered the following error while compiling the regex '"
              + pattern
              + "': "
              + e.getMessage());
    }
  }
}
