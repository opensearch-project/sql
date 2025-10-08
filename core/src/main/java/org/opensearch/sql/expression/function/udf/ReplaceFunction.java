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
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Custom REPLACE function for PPL that substitutes the replacement string for every occurrence of
 * the regular expression in the input string.
 */
public final class ReplaceFunction extends ImplementorUDF {

  public ReplaceFunction() {
    super(new ReplaceImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING_STRING;
  }

  private static class ReplaceImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression str = translatedOperands.get(0);
      Expression regex = translatedOperands.get(1);
      Expression replacement = translatedOperands.get(2);

      return Expressions.call(ReplaceFunction.class, "replaceRegex", str, regex, replacement);
    }
  }

  public static String replaceRegex(String str, String regex, String replacement) {
    if (str == null || regex == null || replacement == null) {
      return null;
    }

    try {
      // Compile regex pattern using Java's Pattern (same as regex_match uses)
      Pattern compiledPattern = Pattern.compile(regex);
      Matcher matcher = compiledPattern.matcher(str);

      // Convert Perl-style \1, \2 to Java-style $1, $2 for capture group references
      // This matches SPL behavior while using Java's regex engine
      String javaReplacement = convertPerlReplacementToJava(replacement);

      return matcher.replaceAll(javaReplacement);

    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Error in 'replace' function: Encountered the following error while compiling the regex '"
              + regex
              + "': "
              + e.getMessage());
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(
          "Error in 'replace' function: Invalid replacement string '"
              + replacement
              + "': "
              + e.getMessage());
    }
  }

  private static String convertPerlReplacementToJava(String perlReplacement) {
    // Replace \1, \2, etc. with $1, $2, etc.
    // We need to be careful with escaped backslashes
    StringBuilder result = new StringBuilder();
    int length = perlReplacement.length();

    for (int i = 0; i < length; i++) {
      char ch = perlReplacement.charAt(i);

      if (ch == '\\' && i + 1 < length) {
        char next = perlReplacement.charAt(i + 1);

        // Check if this is a group reference (\1, \2, etc.)
        if (Character.isDigit(next)) {
          result.append('$').append(next);
          i++; // Skip the next character since we've already processed it
        } else if (next == '\\') {
          // This is an escaped backslash (\\), keep both backslashes
          result.append("\\\\");
          i++; // Skip the next backslash
        } else {
          // This is a backslash followed by some other character, keep as is
          result.append('\\').append(next);
          i++; // Skip the next character
        }
      } else {
        result.append(ch);
      }
    }

    return result.toString();
  }
}
