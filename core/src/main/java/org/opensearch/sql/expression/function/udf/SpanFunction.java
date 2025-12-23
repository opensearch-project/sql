/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.planner.physical.collector.Rounding;
import org.opensearch.sql.planner.physical.collector.Rounding.DateRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimeRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimestampRounding;

public class SpanFunction extends ImplementorUDF {
  public SpanFunction() {
    super(new SpanImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Return arg0 type if it has a unit (i.e. time related span)
    return callBinding -> {
      if (SqlTypeUtil.isString(callBinding.getOperandType(2))) {
        return callBinding.getOperandType(0);
      }
      // Use the least restrictive type between the field type and the interval type if it's a
      // numeric span. E.g. span(int_field, double_literal) -> double
      return callBinding
          .getTypeFactory()
          .leastRestrictive(List.of(callBinding.getOperandType(0), callBinding.getOperandType(1)));
    };
  }

  /**
   * Describe valid operand-type combinations accepted by the SPAN UDF.
   *
   * <p>Accepted signatures:
   * <ul>
   *   <li>(CHARACTER, NUMERIC, CHARACTER)</li>
   *   <li>(DATETIME, NUMERIC, CHARACTER)</li>
   *   <li>(NUMERIC, NUMERIC, ANY)</li>
   * </ul>
   *
   * @return a {@link UDFOperandMetadata} that enforces the above operand families for the SPAN function
   */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)
            .or(
                OperandTypes.family(
                    SqlTypeFamily.DATETIME, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER))
            .or(
                OperandTypes.family(
                    SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY)));
  }

  public static class SpanImplementor implements NotNullImplementor {
    /**
     * Translates a SPAN RexCall into an executable expression for code generation.
     *
     * <p>Behavior:
     * - If the interval type is decimal, the interval operand is converted to double.
     * - If the unit operand is null or has SQL type ANY, returns a numeric rounding expression:
     *   integral SQL return types use (field / interval) * interval; other numeric types use
     *   floor(field / interval) * interval.
     * - If the field type is an ExprSqlType, delegates to the appropriate eval method
     *   (evalDate, evalTime, evalTimestamp) and returns that implementation's expression.
     * - Otherwise, throws IllegalArgumentException for unsupported field expression types.
     *
     * @param translator the RexToLixTranslator used to build expressions
     * @param call the original RexCall for the SPAN invocation
     * @param translatedOperands the already-translated operand expressions (expected size 3)
     * @return an Expression that evaluates the SPAN operation
     * @throws IllegalArgumentException if the field expression type is unsupported or cannot be handled
     */
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      assert call.getOperands().size() == 3 : "SPAN should have 3 arguments";
      assert translatedOperands.size() == 3 : "SPAN should have 3 arguments";
      Expression field = translatedOperands.get(0);
      Expression interval = translatedOperands.get(1);

      RelDataType fieldType = call.getOperands().get(0).getType();
      RelDataType intervalType = call.getOperands().get(1).getType();
      RelDataType unitType = call.getOperands().get(2).getType();

      if (SqlTypeUtil.isDecimal(intervalType)) {
        interval = Expressions.call(interval, "doubleValue");
      }
      if (SqlTypeUtil.isNull(unitType) || SqlTypeName.ANY.equals(unitType.getSqlTypeName())) {
        return switch (call.getType().getSqlTypeName()) {
          case BIGINT, INTEGER, SMALLINT, TINYINT ->
              Expressions.multiply(Expressions.divide(field, interval), interval);
          default ->
              Expressions.multiply(
                  Expressions.call(BuiltInMethod.FLOOR.method, Expressions.divide(field, interval)),
                  interval);
        };
      } else if (fieldType instanceof ExprSqlType exprSqlType) {
        // TODO: pass in constant arguments when constructing
        String methodName =
            switch (exprSqlType.getUdt()) {
              case EXPR_DATE -> "evalDate";
              case EXPR_TIME -> "evalTime";
              case EXPR_TIMESTAMP -> "evalTimestamp";
              default ->
                  throw new IllegalArgumentException(
                      String.format("Unsupported expr type: %s", exprSqlType.getExprType()));
            };
        ScalarFunctionImpl function =
            (ScalarFunctionImpl)
                ScalarFunctionImpl.create(
                    Types.lookupMethod(
                        SpanFunction.class, methodName, String.class, int.class, String.class));
        return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
      }
      throw new IllegalArgumentException(
          String.format(
              "Unsupported expr type: %s",
              OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType)));
    }
  }

  @Strict
  public static Object evalDate(
      @Parameter(name = "value") String value,
      @Parameter(name = "interval") int interval,
      @Parameter(name = "unit") String unit) {
    ExprValue exprInterval = ExprValueUtils.fromObjectValue(interval, ExprCoreType.INTEGER);
    ExprValue exprValue = ExprValueUtils.fromObjectValue(value, ExprCoreType.DATE);
    Rounding<?> rounding = new DateRounding(exprInterval, unit);
    return rounding.round(exprValue).valueForCalcite();
  }

  @Strict
  public static Object evalTime(
      @Parameter(name = "value") String value,
      @Parameter(name = "interval") int interval,
      @Parameter(name = "unit") String unit) {
    ExprValue exprInterval = ExprValueUtils.fromObjectValue(interval, ExprCoreType.INTEGER);
    ExprValue exprValue = ExprValueUtils.fromObjectValue(value, ExprCoreType.TIME);
    Rounding<?> rounding = new TimeRounding(exprInterval, unit);
    return rounding.round(exprValue).valueForCalcite();
  }

  @Strict
  public static Object evalTimestamp(
      @Parameter(name = "value") String value,
      @Parameter(name = "interval") int interval,
      @Parameter(name = "unit") String unit) {
    ExprValue exprInterval = ExprValueUtils.fromObjectValue(interval, ExprCoreType.INTEGER);
    ExprValue exprValue = ExprValueUtils.fromObjectValue(value, ExprCoreType.TIMESTAMP);
    Rounding<?> rounding = new TimestampRounding(exprInterval, unit);
    return rounding.round(exprValue).valueForCalcite();
  }
}