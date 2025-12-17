/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MVFIND function implementation that finds the index of the first element in a multivalue array
 * that matches a regular expression.
 *
 * <p>Usage: mvfind(array, regex)
 *
 * <p>Returns the 0-based index of the first array element matching the regex pattern, or NULL if no
 * match is found.
 *
 * <p>Example: mvfind(array('apple', 'banana', 'apricot'), 'ban.*') returns 1
 */
public class MVFindFunctionImpl extends ImplementorUDF {
  public MVFindFunctionImpl() {
    super(new MVFindImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.INTEGER_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Accept ARRAY and STRING for the regex pattern
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER));
  }

  public static class MVFindImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression arrayExpr = translatedOperands.get(0);
      Expression patternExpr = translatedOperands.get(1);

      // Check if regex pattern is a literal - compile at planning time
      if (call.operands.size() >= 2 && call.operands.get(1) instanceof RexLiteral) {
        RexLiteral patternLiteral = (RexLiteral) call.operands.get(1);
        Expression literalPatternExpr = tryCompileLiteralPattern(patternLiteral, arrayExpr);
        if (literalPatternExpr != null) {
          return literalPatternExpr;
        }
      }

      // For dynamic patterns, use evalWithString
      return Expressions.call(
          Types.lookupMethod(MVFindFunctionImpl.class, "evalWithString", List.class, String.class),
          arrayExpr,
          patternExpr);
    }

    private static Expression tryCompileLiteralPattern(
        RexLiteral patternLiteral, Expression arrayExpr) {
      // Use getValueAs(String.class) to correctly unwrap Calcite NlsString
      String patternString = patternLiteral.getValueAs(String.class);
      if (patternString == null) {
        return null;
      }
      try {
        // Compile pattern at planning time and validate
        Pattern compiledPattern = Pattern.compile(patternString);
        // Generate code that uses the pre-compiled pattern
        return Expressions.call(
            Types.lookupMethod(
                MVFindFunctionImpl.class, "evalWithPattern", List.class, Pattern.class),
            arrayExpr,
            Expressions.constant(compiledPattern, Pattern.class));
      } catch (PatternSyntaxException e) {
        // Convert to IllegalArgumentException so it's treated as a client error (400)
        throw new IllegalArgumentException(
            String.format("Invalid regex pattern '%s': %s", patternString, e.getDescription()), e);
      }
    }
  }

  private static Integer mvfindCore(List<Object> array, Pattern pattern) {
    for (int i = 0; i < array.size(); i++) {
      Object element = array.get(i);
      if (element != null) {
        String strValue = element.toString();
        if (pattern.matcher(strValue).find()) {
          return i; // Return 0-based index
        }
      }
    }
    return null; // No match found
  }

  /**
   * Evaluates mvfind with a pre-compiled Pattern (for literal patterns compiled at planning time).
   * Any runtime exceptions from mvfindCore will propagate unchanged.
   *
   * @param array The array to search
   * @param pattern The pre-compiled regex pattern
   * @return The 0-based index of the first matching element, or null if no match
   */
  public static Integer evalWithPattern(List<Object> array, Pattern pattern) {
    if (array == null || pattern == null) {
      return null;
    }
    return mvfindCore(array, pattern);
  }

  /**
   * Evaluates mvfind with a string pattern (for dynamic patterns at runtime).
   *
   * @param array The array to search
   * @param regex The regex pattern string
   * @return The 0-based index of the first matching element, or null if no match
   */
  public static Integer evalWithString(List<Object> array, String regex) {
    if (array == null || regex == null) {
      return null;
    }
    return mvfind(array, regex);
  }

  /**
   * Evaluates mvfind with a String pattern. Compiles the regex pattern and executes search. Throws
   * IllegalArgumentException for invalid regex patterns; other runtime exceptions propagate
   * unchanged.
   *
   * @param array The array to search
   * @param regex The regex pattern string
   * @return The 0-based index of the first matching element, or null if no match
   * @throws IllegalArgumentException if the regex pattern is invalid
   */
  private static Integer mvfind(List<Object> array, String regex) {
    if (array == null || regex == null) {
      return null;
    }

    Pattern pattern;
    try {
      pattern = Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      // Invalid regex is a client error (400)
      throw new IllegalArgumentException(
          String.format("Invalid regex pattern '%s': %s", regex, e.getDescription()), e);
    }
    return mvfindCore(array, pattern);
  }
}
