/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.planner.physical.collector.Rounding;
import org.opensearch.sql.planner.physical.collector.Rounding.DateRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimeRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimestampRounding;

public class SpanFunctionImpl extends ImplementorUDF {
  protected SpanFunctionImpl() {
    super(new SpanImplementor());
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.ARG0;
  }

  public static class SpanImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      assert call.getOperands().size() == 3 : "SPAN should have 3 arguments";
      assert translatedOperands.size() == 3 : "SPAN should have 3 arguments";
      Expression field = translatedOperands.get(0);
      Expression interval = translatedOperands.get(1);

      RelDataType fieldType = call.getOperands().get(0).getType();
      RelDataType unitType = call.getOperands().get(2).getType();

      if (SqlTypeUtil.isNull(unitType)) {
        return switch (call.getType().getSqlTypeName()) {
          case BIGINT, INTEGER, SMALLINT, TINYINT -> Expressions.multiply(
              Expressions.divide(field, interval), interval);
          default -> Expressions.multiply(
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
              default -> throw new IllegalArgumentException(
                  String.format("Unsupported expr udt: %s", exprSqlType.getUdt()));
            };
        ScalarFunctionImpl function =
            (ScalarFunctionImpl)
                ScalarFunctionImpl.create(
                    Types.lookupMethod(
                        SpanFunctionImpl.class, methodName, String.class, int.class, String.class));
        return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
      }
      throw new IllegalArgumentException(String.format("Unsupported RelDataType: %s", fieldType));
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
