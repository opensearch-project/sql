package org.opensearch.sql.calcite.udf.mathUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

// SqlLibraryOperators.SAFE_DIVIDE and SqlStdOperators.DIVIDE does not satisfy 0 handling rule of
// PPL. Therefore, we implement our versions
public class DivideFunctionImpl extends ImplementorUDF {

  public DivideFunctionImpl() {
    super(new DivideImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.QUOTIENT_FORCE_NULLABLE;
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
      }
      double result = dividend.doubleValue() / divisor.doubleValue();
      return MathUtils.coerceToWidestFloatingType(dividend, divisor, result);
    }
  }
}
