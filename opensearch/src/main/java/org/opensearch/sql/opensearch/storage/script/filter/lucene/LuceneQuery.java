/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;

import com.google.common.collect.ImmutableMap;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.BiFunction;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;

/** Lucene query abstraction that builds Lucene query from function expression. */
public abstract class LuceneQuery {

  /**
   * Check if function expression supported by current Lucene query. Default behavior is that report
   * supported if:
   *
   * <ol>
   *   <li>Left is a reference
   *   <li>Right side is a literal
   * </ol>
   *
   * @param func function
   * @return return true if supported, otherwise false.
   */
  public boolean canSupport(FunctionExpression func) {
    return (func.getArguments().size() == 2)
            && (func.getArguments().get(0) instanceof ReferenceExpression)
            && (func.getArguments().get(1) instanceof LiteralExpression
                || literalExpressionWrappedByCast(func))
        || isMultiParameterQuery(func);
  }

  /**
   * Check if predicate expression has nested function on left side of predicate expression.
   * Validation for right side being a `LiteralExpression` is done in NestedQuery.
   *
   * @param func function.
   * @return return true if function has supported nested function expression.
   */
  public boolean isNestedPredicate(FunctionExpression func) {
    return isNestedFunction(func.getArguments().get(0));
  }

  /**
   * Check if the function expression has multiple named argument expressions as the parameters.
   *
   * @param func function
   * @return return true if the expression is a multi-parameter function.
   */
  private boolean isMultiParameterQuery(FunctionExpression func) {
    for (Expression expr : func.getArguments()) {
      if (!(expr instanceof NamedArgumentExpression)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the second argument of the function is a literal expression wrapped by cast function.
   */
  private boolean literalExpressionWrappedByCast(FunctionExpression func) {
    if (func.getArguments().get(1) instanceof FunctionExpression) {
      FunctionExpression expr = (FunctionExpression) func.getArguments().get(1);
      return castMap.containsKey(expr.getFunctionName())
          && expr.getArguments().get(0) instanceof LiteralExpression;
    }
    return false;
  }

  /**
   * Build Lucene query from function expression. The cast function is converted to literal
   * expressions before generating DSL.
   *
   * @param func function
   * @return query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue =
        expr instanceof LiteralExpression ? expr.valueOf() : cast((FunctionExpression) expr, ref);
    return doBuild(ref.getRawPath(), ref.type(), literalValue);
  }

  private ExprValue cast(FunctionExpression castFunction, ReferenceExpression ref) {
    return castMap
        .get(castFunction.getFunctionName())
        .apply((LiteralExpression) castFunction.getArguments().get(0), ref);
  }

  /** Type converting map. */
  private final Map<FunctionName, BiFunction<LiteralExpression, ReferenceExpression, ExprValue>>
      castMap =
          ImmutableMap
              .<FunctionName, BiFunction<LiteralExpression, ReferenceExpression, ExprValue>>
                  builder()
              .put(
                  BuiltinFunctionName.CAST_TO_STRING.getName(),
                  (expr, ref) -> {
                    if (!expr.type().equals(ExprCoreType.STRING)) {
                      return new ExprStringValue(String.valueOf(expr.valueOf().value()));
                    } else {
                      return expr.valueOf();
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_BYTE.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprByteValue(expr.valueOf().byteValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprByteValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprByteValue(Byte.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_SHORT.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprShortValue(expr.valueOf().shortValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprShortValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprShortValue(Short.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_INT.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprIntegerValue(expr.valueOf().integerValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprIntegerValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprIntegerValue(Integer.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_LONG.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprLongValue(expr.valueOf().longValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprLongValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprLongValue(Long.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_FLOAT.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprFloatValue(expr.valueOf().floatValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprFloatValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprFloatValue(Float.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_DOUBLE.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return new ExprDoubleValue(expr.valueOf().doubleValue());
                    } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
                      return new ExprDoubleValue(expr.valueOf().booleanValue() ? 1 : 0);
                    } else {
                      return new ExprDoubleValue(Double.valueOf(expr.valueOf().stringValue()));
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_BOOLEAN.getName(),
                  (expr, ref) -> {
                    if (ExprCoreType.numberTypes().contains(expr.type())) {
                      return expr.valueOf().doubleValue() != 0
                          ? ExprBooleanValue.of(true)
                          : ExprBooleanValue.of(false);
                    } else if (expr.type().equals(ExprCoreType.STRING)) {
                      return ExprBooleanValue.of(Boolean.valueOf(expr.valueOf().stringValue()));
                    } else {
                      return expr.valueOf();
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_IP.getName(),
                  (expr, ref) -> {
                    return new ExprIpValue(expr.valueOf().stringValue());
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_DATE.getName(),
                  (expr, ref) -> {
                    if (expr.type().equals(ExprCoreType.STRING)) {
                      ZonedDateTime zonedDateTime = getParsedDateTime(expr, ref);
                      if (zonedDateTime != null) {
                        return new ExprDateValue(zonedDateTime.toLocalDate());
                      }
                      return new ExprDateValue(expr.valueOf().stringValue());
                    } else {
                      return new ExprDateValue(expr.valueOf().dateValue());
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_TIME.getName(),
                  (expr, ref) -> {
                    if (expr.type().equals(ExprCoreType.STRING)) {
                      ZonedDateTime zonedDateTime = getParsedDateTime(expr, ref);
                      if (zonedDateTime != null) {
                        return new ExprTimeValue(zonedDateTime.toLocalTime());
                      }
                      return new ExprTimeValue(expr.valueOf().stringValue());
                    } else {
                      return new ExprTimeValue(expr.valueOf().timeValue());
                    }
                  })
              .put(
                  BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(),
                  (expr, ref) -> {
                    if (expr.type().equals(ExprCoreType.STRING)) {
                      ZonedDateTime zonedDateTime = getParsedDateTime(expr, ref);
                      if (zonedDateTime != null) {
                        return new ExprTimestampValue(zonedDateTime.toInstant());
                      }
                      return new ExprTimestampValue(expr.valueOf().stringValue());
                    } else {
                      return new ExprTimestampValue(expr.valueOf().timestampValue());
                    }
                  })
              .build();

  /**
   * Parses the date/time from the given expression if the reference type is an instance of
   * OpenSearchDateType.
   *
   * @param expr The expression to parse.
   * @return The parsed ZonedDateTime or null if the conditions are not met.
   */
  private ZonedDateTime getParsedDateTime(LiteralExpression expr, ReferenceExpression ref) {
    if (ref.type() instanceof OpenSearchDateType) {
      return ((OpenSearchDateType) ref.type()).getParsedDateTime(expr.valueOf().stringValue());
    }
    return null;
  }

  /**
   * Build method that subclass implements by default which is to build query from reference and
   * literal in function arguments.
   *
   * @param fieldName field name
   * @param fieldType field type
   * @param literal field value literal
   * @return query
   */
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    throw new UnsupportedOperationException(
        "Subclass doesn't implement this and build method either");
  }

  /**
   * Converts a literal value to a formatted date or time value based on the specified field type.
   *
   * <p>If the field type is an instance of {@link OpenSearchDateType}, this method checks the type
   * of the literal value and converts it to a formatted date or time if necessary. The formatting
   * is applied if the {@link OpenSearchDateType} has a formatter. Otherwise, the raw value is
   * returned.
   *
   * @param literal the literal value to be converted
   * @param fieldType the field type to determine the conversion logic
   * @return the formatted date or time value if the field type requires it, otherwise the raw value
   */
  protected Object value(ExprValue literal, ExprType fieldType) {
    if (fieldType instanceof OpenSearchDateType) {
      OpenSearchDateType openSearchDateType = (OpenSearchDateType) fieldType;
      if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
        return openSearchDateType.hasNoFormatter()
            ? literal.timestampValue().toEpochMilli()
            : openSearchDateType.getFormattedDate(literal.timestampValue());
      } else if (literal.type().equals(ExprCoreType.DATE)) {
        return openSearchDateType.hasNoFormatter()
            ? literal.value()
            : openSearchDateType.getFormattedDate(literal.dateValue());
      } else if (literal.type().equals(ExprCoreType.TIME)) {
        return openSearchDateType.hasNoFormatter()
            ? literal.value()
            : openSearchDateType.getFormattedDate(literal.timeValue());
      }
    }
    return literal.value();
  }
}
