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
    return ((func.getArguments().size() == 2)
            && (func.getArguments().get(0) instanceof ReferenceExpression
                || referenceWrappedByRedundantDateCast(func.getArguments().get(0)))
            && (func.getArguments().get(1) instanceof LiteralExpression
                || literalExpressionWrappedByCast(func)))
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
   * Check if the value operand of the function is a literal wrapped in conversions that can be
   * evaluated up front, so the comparison can still push down instead of falling back to a script.
   */
  protected boolean literalExpressionWrappedByCast(FunctionExpression func) {
    return resolveLiteralOperand(
            func.getArguments().get(1), fieldDateType(func.getArguments().get(0)))
        != null;
  }

  /**
   * The date/time type of the filtered field, or null when it is not a date/time field. Used to
   * keep a date conversion on the bound from being resolved against a field of a different
   * date/time type, which would silently change the bound (for example resolving a {@code
   * timestamp('...')} bound against a {@code DATE} field drops the time component).
   */
  private ExprCoreType fieldDateType(Expression arg) {
    Expression ref =
        referenceWrappedByRedundantDateCast(arg)
            ? ((FunctionExpression) arg).getArguments().get(0)
            : arg;
    return ref.type() instanceof OpenSearchDateType dateType ? dateType.getExprCoreType() : null;
  }

  /**
   * The {@code timestamp()}/{@code date()}/{@code time()} builtins and the cast they are equal to.
   */
  private static final Map<FunctionName, FunctionName> DATE_CONVERSION_TO_CAST =
      ImmutableMap.of(
          BuiltinFunctionName.TIMESTAMP.getName(),
          BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(),
          BuiltinFunctionName.DATE.getName(),
          BuiltinFunctionName.CAST_TO_DATE.getName(),
          BuiltinFunctionName.TIME.getName(),
          BuiltinFunctionName.CAST_TO_TIME.getName());

  /**
   * Resolve the value operand to the cast that should be evaluated and the literal to evaluate it
   * on, or null when the operand is not a literal behind supported conversions.
   *
   * <p>Besides the plain {@code CAST_TO_*(literal)} form, this accepts the {@code
   * timestamp()}/{@code date()}/{@code time()} builtins, which are what clients typically emit for
   * a time-range bound (for example the Grafana OpenSearch data source builds its PPL time filter
   * as {@code where `field` >= timestamp('...')}). PPL coerces the string argument first, so the
   * operand arrives as {@code timestamp(cast_to_timestamp('...'))}; applying {@code timestamp()} to
   * a value that is already a timestamp is a no-op, so the inner cast alone is evaluated. The cast
   * is resolved rather than evaluated here so that {@link #castMap} keeps parsing the literal
   * against the field's declared date formats.
   */
  private Map.Entry<FunctionName, LiteralExpression> resolveLiteralOperand(
      Expression expr, ExprCoreType fieldDateType) {
    if (!(expr instanceof FunctionExpression fn)) {
      return null;
    }
    FunctionName name = fn.getFunctionName();
    Expression inner = fn.getArguments().isEmpty() ? null : fn.getArguments().get(0);

    if (castMap.containsKey(name) && inner instanceof LiteralExpression literal) {
      return Map.entry(name, literal);
    }
    FunctionName equivalentCast = DATE_CONVERSION_TO_CAST.get(name);
    if (equivalentCast == null || fn.getArguments().size() != 1) {
      return null;
    }
    // Only resolve the conversion when it produces the type the field already has. Resolving across
    // date/time types would re-format the bound into the field's format and change which documents
    // match, so those are left on the script path.
    if (!DATE_CAST_TARGET_TYPES.get(name).equals(fieldDateType)) {
      return null;
    }
    if (inner instanceof LiteralExpression literal) {
      return Map.entry(equivalentCast, literal);
    }
    if (inner instanceof FunctionExpression innerFn
        && innerFn.getArguments().size() == 1
        && innerFn.getArguments().get(0) instanceof LiteralExpression literal
        && castMap.containsKey(innerFn.getFunctionName())
        && DATE_CAST_TARGET_TYPES
            .get(name)
            .equals(DATE_CAST_TARGET_TYPES.get(innerFn.getFunctionName()))) {
      return Map.entry(innerFn.getFunctionName(), literal);
    }
    return null;
  }

  /** Date/time cast functions mapped to the type each one produces. */
  private static final Map<FunctionName, ExprCoreType> DATE_CAST_TARGET_TYPES =
      ImmutableMap.<FunctionName, ExprCoreType>builder()
          .put(BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(), ExprCoreType.TIMESTAMP)
          .put(BuiltinFunctionName.TIMESTAMP.getName(), ExprCoreType.TIMESTAMP)
          .put(BuiltinFunctionName.CAST_TO_DATE.getName(), ExprCoreType.DATE)
          .put(BuiltinFunctionName.DATE.getName(), ExprCoreType.DATE)
          .put(BuiltinFunctionName.CAST_TO_TIME.getName(), ExprCoreType.TIME)
          .put(BuiltinFunctionName.TIME.getName(), ExprCoreType.TIME)
          .build();

  /**
   * Check if the left operand is a date/time cast (or the {@code timestamp()}/{@code date()}/{@code
   * time()} builtin) applied to a reference of <b>the same</b> date/time type. Such a wrap is a
   * no-op, so it can be unwrapped, letting the predicate push down to a native range/term query
   * instead of falling back to a per-document script.
   *
   * <p>The cast target must match the field type exactly. A cast that changes the date/time type is
   * a real conversion and must not be folded: {@code date(<timestamp field>)} truncates the time
   * component (so {@code date(ts) <= '2024-01-15'} is not {@code ts <= '2024-01-15'}), and {@code
   * time(<timestamp field>)} extracts the time of day, which is not even monotonic with respect to
   * the timestamp.
   *
   * @param arg left operand of the comparison.
   * @return true if the operand is a redundant, order-preserving date/time cast over a reference.
   */
  protected boolean referenceWrappedByRedundantDateCast(Expression arg) {
    if (!(arg instanceof FunctionExpression)) {
      return false;
    }
    FunctionExpression fn = (FunctionExpression) arg;
    ExprCoreType castTarget = DATE_CAST_TARGET_TYPES.get(fn.getFunctionName());
    if (castTarget == null || fn.getArguments().size() != 1) {
      return false;
    }
    Expression inner = fn.getArguments().get(0);
    return inner instanceof ReferenceExpression
        && inner.type() instanceof OpenSearchDateType dateType
        && castTarget.equals(dateType.getExprCoreType());
  }

  /**
   * Return the underlying reference of the left operand, unwrapping a redundant date/time cast if
   * present (see {@link #referenceWrappedByRedundantDateCast}). Callers must ensure {@link
   * #canSupport} returned true for the enclosing function; otherwise an {@link
   * IllegalStateException} is thrown rather than allowing an unchecked cast to fail.
   */
  private ReferenceExpression unwrapReference(Expression arg) {
    if (arg instanceof ReferenceExpression) {
      return (ReferenceExpression) arg;
    }
    if (referenceWrappedByRedundantDateCast(arg)) {
      return (ReferenceExpression) ((FunctionExpression) arg).getArguments().get(0);
    }
    throw new IllegalStateException(
        "Left operand must be a reference or a redundant date/time cast over a reference; "
            + "canSupport() must be checked before build()");
  }

  /**
   * Build Lucene query from function expression. The cast function is converted to literal
   * expressions before generating DSL.
   *
   * @param func function
   * @return query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = unwrapReference(func.getArguments().get(0));
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue =
        expr instanceof LiteralExpression ? expr.valueOf() : cast((FunctionExpression) expr, ref);
    if (func.getArguments().size() == 3) {
      return doBuild(
          ref.getRawPath(), ref.type(), literalValue, func.getArguments().get(2).valueOf());
    }
    return doBuild(ref.getRawPath(), ref.type(), literalValue);
  }

  private ExprValue cast(FunctionExpression castFunction, ReferenceExpression ref) {
    Map.Entry<FunctionName, LiteralExpression> resolved =
        resolveLiteralOperand(castFunction, fieldDateType(ref));
    if (resolved == null) {
      throw new IllegalStateException(
          "Value operand must be a literal behind supported conversions; canSupport() must be"
              + " checked before build()");
    }
    return castMap.get(resolved.getKey()).apply(resolved.getValue(), ref);
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

  protected QueryBuilder doBuild(
      String fieldName, ExprType fieldType, ExprValue literal1, ExprValue literal2) {
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
