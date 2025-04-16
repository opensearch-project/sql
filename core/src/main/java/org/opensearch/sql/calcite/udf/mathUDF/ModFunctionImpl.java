/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class ModFunctionImpl extends ImplementorUDF {
  public ModFunctionImpl() {
    super(new ModImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class ModImplementor implements NotNullImplementor {

    /**
     * Implements a call with assumption that all the null-checking is implemented by caller.
     *
     * @param translator translator to implement the code
     * @param call call to implement
     * @param translatedOperands arguments of a call
     * @return expression that implements given call
     */
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression dividend = translatedOperands.get(0);
      Expression divisor = translatedOperands.get(1);
      RelDataType dividendType = call.getOperands().get(0).getType();
      RelDataType divisorType = call.getOperands().get(1).getType();
      // If both dividend and divisor are integral type, use builtin MOD
      if (SqlTypeFamily.INTEGER.contains(dividendType)
          && SqlTypeFamily.INTEGER.contains(divisorType)) {
        return Expressions.call(
            ModImplementor.class,
            "integralMod",
            Expressions.convert_(dividend, Number.class),
            Expressions.convert_(divisor, Number.class));
      } else {
        return Expressions.call(
            ModImplementor.class,
            "floatingMod",
            Expressions.convert_(dividend, Number.class),
            Expressions.convert_(divisor, Number.class));
      }
    }

    public static Number integralMod(Number dividend, Number divisor) {
      if (divisor.doubleValue() == 0) {
        return null;
      }
      long l0 = dividend.longValue();
      long l1 = divisor.longValue();
      // It returns negative values when l0 is negative
      long result = l0 % l1;
      // Return the wider type between l0 and l1
      return MathUtils.coerceToWidestIntegralType(dividend, divisor, result);
    }

    public static Number floatingMod(Number dividend, Number divisor) {
      if (divisor.doubleValue() == 0) {
        return null;
      }
      BigDecimal b0 = new BigDecimal(dividend.toString());
      BigDecimal b1 = new BigDecimal(divisor.toString());
      BigDecimal result = b0.remainder(b1);
      return MathUtils.coerceToWidestFloatingType(dividend, divisor, result.doubleValue());
    }
  }
}
