/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Base class for PPL conversion functions. */
@Log4j2
public abstract class BaseConversionUDF extends ImplementorUDF {

  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern LEADING_NUMBER_WITH_UNIT_PATTERN =
      Pattern.compile("^([+-]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][+-]?\\d+)?)(.*)$");
  private static final Pattern CONTAINS_LETTER_PATTERN = Pattern.compile(".*[a-zA-Z].*");
  private static final Pattern STARTS_WITH_SIGN_OR_DIGIT = Pattern.compile("^[+-]?[\\d.].*");
  private static final Pattern MEMK_PATTERN = Pattern.compile("^([+-]?\\d+\\.?\\d*)([kmgKMG])?$");

  private static final double MB_TO_KB = 1024.0;
  private static final double GB_TO_KB = 1024.0 * 1024.0;

  protected BaseConversionUDF(Class<? extends BaseConversionUDF> functionClass) {
    super(new ConversionImplementor(functionClass), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        factory ->
            factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE), true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.OPTIONAL_ANY;
  }

  /**
   * Template method defining the conversion algorithm structure. Subclasses implement
   * applyConversion() to provide specific conversion logic.
   */
  public final Object convertValue(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    return applyConversion(str);
  }

  /**
   * Abstract method for subclasses to implement their specific conversion logic.
   *
   * @param preprocessedValue The preprocessed string value
   * @return The converted value or null if conversion fails
   */
  protected abstract Object applyConversion(String preprocessedValue);

  // String processing helpers
  protected String preprocessValue(Object value) {
    if (value == null) {
      return null;
    }
    String str = value instanceof String ? ((String) value).trim() : value.toString().trim();
    return str.isEmpty() ? null : str;
  }

  protected String extractLeadingNumber(String str) {
    Matcher matcher = LEADING_NUMBER_WITH_UNIT_PATTERN.matcher(str);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }

  protected boolean containsLetter(String str) {
    return CONTAINS_LETTER_PATTERN.matcher(str).matches();
  }

  protected boolean isPotentiallyConvertible(String str) {
    return STARTS_WITH_SIGN_OR_DIGIT.matcher(str).matches();
  }

  protected boolean hasValidUnitSuffix(String str, String leadingNumber) {
    if (leadingNumber == null || leadingNumber.length() >= str.length()) {
      return false;
    }
    String suffix = str.substring(leadingNumber.length()).trim();
    if (suffix.isEmpty()) {
      return false;
    }
    char firstChar = suffix.charAt(0);
    return !Character.isDigit(firstChar) && firstChar != '.';
  }

  // Number parsing helpers
  protected Double tryParseDouble(String str) {
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException e) {
      log.debug("Failed to parse '{}' as number", str, e);
      return null;
    }
  }

  protected Double tryConvertWithCommaRemoval(String str) {
    String noCommas = COMMA_PATTERN.matcher(str).replaceAll("");
    return tryParseDouble(noCommas);
  }

  protected Double tryConvertMemoryUnit(String str) {
    Matcher matcher = MEMK_PATTERN.matcher(str);
    if (!matcher.matches()) {
      return null;
    }

    Double number = tryParseDouble(matcher.group(1));
    if (number == null) {
      return null;
    }

    String unit = matcher.group(2);
    if (unit == null || unit.equalsIgnoreCase("k")) {
      return number;
    }

    double multiplier =
        switch (unit.toLowerCase()) {
          case "m" -> MB_TO_KB;
          case "g" -> GB_TO_KB;
          default -> 1.0;
        };

    return number * multiplier;
  }

  // Calcite integration
  public static class ConversionImplementor implements NotNullImplementor {
    private final Class<? extends BaseConversionUDF> functionClass;

    public ConversionImplementor(Class<? extends BaseConversionUDF> functionClass) {
      this.functionClass = functionClass;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        return Expressions.call(
            ConversionImplementor.class,
            "toDoubleOrNull",
            Expressions.constant(null, Object.class));
      }

      Expression fieldValue = translatedOperands.get(0);
      Expression result = Expressions.call(functionClass, "convert", Expressions.box(fieldValue));
      return Expressions.call(ConversionImplementor.class, "toDoubleOrNull", result);
    }

    public static Double toDoubleOrNull(Object value) {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      return null;
    }
  }
}
