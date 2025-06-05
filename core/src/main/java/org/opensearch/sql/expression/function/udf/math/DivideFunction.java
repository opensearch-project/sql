/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation for division function.
 *
 * <p>SqlLibraryOperators.SAFE_DIVIDE and SqlStdOperators.DIVIDE does not satisfy 0 handling rule of
 * PPL. Therefore, we implement our versions
 */
public class DivideFunction extends ImplementorUDF {
  /**
   * The maximum scale of numeric which is aligned with {@link
   * RelDataTypeSystem#getMaxScale(SqlTypeName)}. TODO The max scale in Spark is 38, but the default
   * max scale in Calcite is 19.
   */
  public static final int MAX_NUMERIC_SCALE = 19;

  public DivideFunction() {
    super(new DivideImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.QUOTIENT_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC;
  }

  public static class DivideImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          DivideImplementor.class,
          "divide",
          Expressions.convert_(translatedOperands.get(0), Number.class),
          Expressions.convert_(translatedOperands.get(1), Number.class));
    }

    public static Number divide(Number dividend, Number divisor) {

      if (divisor.doubleValue() == 0) {
        return null;
      }

      if (MathUtils.isIntegral(dividend) && MathUtils.isIntegral(divisor)) {
        long result = dividend.longValue() / divisor.longValue();
        return MathUtils.coerceToWidestIntegralType(dividend, divisor, result);
      } else if (MathUtils.isDecimal(dividend) && MathUtils.isIntegral(divisor)) {
        return ((BigDecimal) dividend)
            .divide(
                BigDecimal.valueOf(divisor.longValue()),
                MAX_NUMERIC_SCALE + 1,
                RoundingMode.HALF_UP);
      } else if (MathUtils.isIntegral(dividend) && MathUtils.isDecimal(divisor)) {
        return (BigDecimal.valueOf(dividend.longValue()))
            .divide((BigDecimal) divisor, MAX_NUMERIC_SCALE + 1, RoundingMode.HALF_UP);
      } else if (MathUtils.isDecimal(dividend) && MathUtils.isDecimal(divisor)) {
        return ((BigDecimal) dividend)
            .divide((BigDecimal) divisor, MAX_NUMERIC_SCALE + 1, RoundingMode.HALF_UP);
      }
      double result = dividend.doubleValue() / divisor.doubleValue();
      return MathUtils.coerceToWidestFloatingType(dividend, divisor, result);
    }
  }
}
