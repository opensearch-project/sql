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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Custom REX_OFFSET function for calculating regex match positions. */
public final class RexOffsetFunction extends ImplementorUDF {

  public RexOffsetFunction() {
    super(new RexOffsetImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000_NULLABLE;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING;
  }

  private static class RexOffsetImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression pattern = translatedOperands.get(1);

      return Expressions.call(RexOffsetFunction.class, "calculateOffsets", field, pattern);
    }
  }

  public static String calculateOffsets(String text, String patternStr) {
    if (text == null || patternStr == null) {
      return null;
    }

    try {
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(text);

      if (!matcher.find()) {
        return null;
      }

      List<String> offsetPairs = new java.util.ArrayList<>();

      Pattern namedGroupPattern = Pattern.compile("\\(\\?<([^>]+)>");
      Matcher namedGroupMatcher = namedGroupPattern.matcher(patternStr);

      int groupIndex = 1;

      while (namedGroupMatcher.find()) {
        String groupName = namedGroupMatcher.group(1);

        if (groupIndex <= matcher.groupCount()) {
          int start = matcher.start(groupIndex);
          int end = matcher.end(groupIndex);

          if (start >= 0 && end >= 0) {
            offsetPairs.add(groupName + "=" + start + "-" + (end - 1));
          }
        }
        groupIndex++;
      }

      java.util.Collections.sort(offsetPairs);
      return offsetPairs.isEmpty() ? null : String.join("&", offsetPairs);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid regex pattern in rex command: " + e.getMessage(), e);
    }
  }
}
