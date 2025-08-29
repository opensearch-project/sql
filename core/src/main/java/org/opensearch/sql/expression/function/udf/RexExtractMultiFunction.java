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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Custom REX_EXTRACT_MULTI function for extracting multiple regex matches. */
public final class RexExtractMultiFunction extends ImplementorUDF {

  public RexExtractMultiFunction() {
    super(new RexExtractMultiImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return call ->
        call.getTypeFactory()
            .createArrayType(call.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 2000), -1);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING_INTEGER_INTEGER;
  }

  private static class RexExtractMultiImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression pattern = translatedOperands.get(1);
      Expression groupIndex = translatedOperands.get(2);
      Expression maxMatch = translatedOperands.get(3);

      return Expressions.call(
          RexExtractMultiFunction.class,
          "extractMultipleGroups",
          field,
          pattern,
          groupIndex,
          maxMatch);
    }
  }

  public static List<String> extractMultipleGroups(
      String text, String pattern, int groupIndex, int maxMatch) {
    // Query planner already validates null inputs via NullPolicy.ARG0
    try {
      Pattern compiledPattern = Pattern.compile(pattern);
      Matcher matcher = compiledPattern.matcher(text);
      List<String> matches = new ArrayList<>();

      int matchCount = 0;
      while (matcher.find() && (maxMatch == 0 || matchCount < maxMatch)) {
        if (groupIndex > 0 && groupIndex <= matcher.groupCount()) {
          String match = matcher.group(groupIndex);
          if (match != null) {
            matches.add(match);
            matchCount++;
          }
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
