/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.expression.parse.RegexCommonUtils;

/** Custom REX_EXTRACT function for extracting regex named capture groups. */
public final class RexExtractFunction extends ImplementorUDF {

  public RexExtractFunction() {
    super(new RexExtractImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000_NULLABLE;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // Support both (field, pattern, groupIndex) and (field, pattern, groupName)
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            PPLOperandTypes.STRING_STRING_INTEGER
                .getInnerTypeChecker()
                .or(PPLOperandTypes.STRING_STRING_STRING.getInnerTypeChecker()));
  }

  private static class RexExtractImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression pattern = translatedOperands.get(1);
      Expression groupIndexOrName = translatedOperands.get(2);

      return Expressions.call(
          RexExtractFunction.class, "extractGroup", field, pattern, groupIndexOrName);
    }
  }

  /**
   * Extract a regex group by index (1-based).
   *
   * @param text The input text to extract from
   * @param pattern The regex pattern
   * @param groupIndex The 1-based group index to extract
   * @return The extracted value or null if not found or invalid
   */
  public static String extractGroup(String text, String pattern, int groupIndex) {
    if (text == null || pattern == null) {
      return null;
    }

    return executeExtraction(
        text,
        pattern,
        matcher -> {
          if (groupIndex > 0 && groupIndex <= matcher.groupCount()) {
            return matcher.group(groupIndex);
          }
          return null;
        });
  }

  /**
   * Extract a named capture group from text using the provided pattern. This method avoids the
   * index shifting issue that occurs with nested unnamed groups.
   *
   * @param text The input text to extract from
   * @param pattern The regex pattern with named capture groups
   * @param groupName The name of the capture group to extract
   * @return The extracted value or null if not found
   */
  public static String extractGroup(String text, String pattern, String groupName) {
    if (text == null || pattern == null || groupName == null) {
      return null;
    }

    return executeExtraction(
        text,
        pattern,
        matcher -> {
          try {
            return matcher.group(groupName);
          } catch (IllegalArgumentException e) {
            // Group name doesn't exist in the pattern
            return null;
          }
        });
  }

  /**
   * Common extraction logic to avoid code duplication.
   *
   * @param text The input text
   * @param pattern The regex pattern
   * @param extractor Function to extract the value from the matcher
   * @return The extracted value or null
   */
  private static String executeExtraction(
      String text, String pattern, java.util.function.Function<Matcher, String> extractor) {
    try {
      Pattern compiledPattern = RegexCommonUtils.getCompiledPattern(pattern);
      Matcher matcher = compiledPattern.matcher(text);

      if (matcher.find()) {
        return extractor.apply(matcher);
      }
      return null;
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Error in 'rex' command: Encountered the following error while compiling the regex '"
              + pattern
              + "': "
              + e.getMessage());
    }
  }
}
