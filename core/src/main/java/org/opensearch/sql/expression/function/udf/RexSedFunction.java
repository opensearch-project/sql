/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Custom REX_SED function for string replacement using sed expressions. */
public final class RexSedFunction extends ImplementorUDF {

  public RexSedFunction() {
    super(new RexSedImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING;
  }

  private static class RexSedImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression sedExpression = translatedOperands.get(1);

      return Expressions.call(RexSedFunction.class, "applySed", field, sedExpression);
    }
  }

  public static String applySed(String text, String sedExpression) {
    if (text == null || sedExpression == null) {
      return null;
    }

    try {
      if (sedExpression.startsWith("s/")) {
        return applySubstitution(text, sedExpression);
      } else if (sedExpression.startsWith("y/")) {
        return applyTransliteration(text, sedExpression);
      } else {
        return text;
      }
    } catch (Exception e) {
      return text;
    }
  }

  private static String applySubstitution(String text, String sedExpression) {
    Pattern sedPattern = Pattern.compile("s/(.*?)/(.*?)/(.*?)$");
    Matcher sedMatcher = sedPattern.matcher(sedExpression);

    if (!sedMatcher.matches()) {
      return text;
    }

    String regex = sedMatcher.group(1);
    String replacement = sedMatcher.group(2);
    String flags = sedMatcher.group(3);

    try {
      Pattern regexPattern = Pattern.compile(regex);
      String javaReplacement = convertSedBackreferencesToJava(replacement);

      if (flags.contains("g")) {
        return regexPattern.matcher(text).replaceAll(javaReplacement);
      } else if (flags.matches("\\d+")) {
        int nth = Integer.parseInt(flags);
        return replaceNthOccurrence(text, regexPattern, javaReplacement, nth);
      } else {
        return regexPattern.matcher(text).replaceFirst(javaReplacement);
      }
    } catch (Exception e) {
      return text;
    }
  }

  private static String applyTransliteration(String text, String sedExpression) {
    Pattern yPattern = Pattern.compile("y/(.*?)/(.*?)/");
    Matcher yMatcher = yPattern.matcher(sedExpression);

    if (!yMatcher.matches()) {
      return text;
    }

    String from = yMatcher.group(1);
    String to = yMatcher.group(2);

    if (from.length() != to.length()) {
      return text;
    }

    StringBuilder result = new StringBuilder();
    for (char c : text.toCharArray()) {
      int index = from.indexOf(c);
      if (index >= 0) {
        result.append(to.charAt(index));
      } else {
        result.append(c);
      }
    }

    return result.toString();
  }

  private static String replaceNthOccurrence(
      String text, Pattern pattern, String replacement, int nth) {
    Matcher matcher = pattern.matcher(text);
    StringBuffer result = new StringBuffer();
    int count = 0;

    while (matcher.find()) {
      count++;
      if (count == nth) {
        matcher.appendReplacement(result, replacement);
      } else {
        matcher.appendReplacement(result, matcher.group());
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }

  private static String convertSedBackreferencesToJava(String sedReplacement) {
    return sedReplacement.replaceAll("\\\\(\\d+)", "\\$$1");
  }
}
