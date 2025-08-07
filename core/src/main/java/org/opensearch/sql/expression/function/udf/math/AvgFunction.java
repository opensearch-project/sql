package org.opensearch.sql.expression.function.udf.math;

import static org.opensearch.sql.expression.function.udf.math.DivideFunction.MAX_NUMERIC_SCALE;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.*;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Variadic AVG function for row-wise average of numeric values in eval/where contexts.
 *
 * <p>This function enables calculations like: - eval average = avg(fieldA, fieldB, fieldC) ->
 * row-wise average: (fieldA + fieldB + fieldC) / 3 - eval average = avg(2, 3, 4) -> integer
 * average: (2 + 3 + 4) / 3 = 3.0 (DOUBLE) - where avg(score1, score2, score3) > 80
 *
 * <p>Key behaviors: - Always returns DOUBLE: avg(2, 3) = 2.5 (preserves decimal precision) - Null
 * handling: If ANY argument is null, result is null (NullPolicy.ANY) - Type inference: Always
 * returns DOUBLE regardless of input types
 *
 * <p>Note: This function only handles scalar numeric values. Array fields are not supported.
 *
 * <p>Different from aggregation avg() which operates across rows: - stats avg(fieldA) by host ->
 * aggregation across multiple rows
 */
public class AvgFunction extends ImplementorUDF {

  public AvgFunction() {
    // NullPolicy.ANY: If any argument is null, result is null
    // This allows NotNullImplementor to assume all args are non-null
    super(new AvgImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Always return DOUBLE for averages to preserve precision
    // Examples: avg(1, 2) → 1.5 (DOUBLE), avg(1, 2.5) → 1.75 (DOUBLE)
    // This ensures integer averages don't lose decimal precision
    // Apply TO_NULLABLE transform to handle null operands correctly
    return ReturnTypes.DOUBLE.andThen(SqlTypeTransforms.TO_NULLABLE);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Use the new type checker that allows 1 or more numeric arguments
    // This works with our enhanced PPLCompositeTypeChecker that supports AND composition
    return PPLOperandTypes.VARIADIC_NUMERIC;
  }

  /**
   * NotNullImplementor assumes all arguments are non-null (null checking done by NullPolicy) This
   * is more efficient than manually checking nulls in our implementation
   */
  public static class AvgImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        // avg() with no arguments should throw an exception
        throw new IllegalArgumentException("AVG function requires at least one argument");
      }

      // Check if any operand is BigDecimal to determine execution strategy
      boolean anyDecimal = false;

      for (int i = 0; i < call.getOperands().size(); i++) {
        RelDataType operandType = call.getOperands().get(i).getType();
        if (operandType.getSqlTypeName() == SqlTypeName.DECIMAL) {
          anyDecimal = true;
          break;
        }
      }

      if (anyDecimal) {
        // For BigDecimal operands, use high-precision BigDecimal arithmetic
        Expression[] operands = new Expression[translatedOperands.size()];
        for (int i = 0; i < translatedOperands.size(); i++) {
          operands[i] = Expressions.convert_(translatedOperands.get(i), Number.class);
        }

        return Expressions.call(
            AvgImplementor.class,
            "bigDecimalAverage",
            Expressions.newArrayInit(Number.class, operands));
      } else {
        // For all other types, use direct expression addition and division
        Expression sum = translatedOperands.get(0);
        for (int i = 1; i < translatedOperands.size(); i++) {
          sum = Expressions.add(sum, translatedOperands.get(i));
        }

        // Convert sum to double and divide by count
        Expression doubleSum = Expressions.convert_(sum, double.class);
        Expression count = Expressions.constant((double) translatedOperands.size());
        return Expressions.divide(doubleSum, count);
      }
    }

    public static Number bigDecimalAverage(Number[] operands) {
      // Use BigDecimal for high precision calculation (called when BigDecimal detected at compile
      // time)
      BigDecimal sum = new BigDecimal(operands[0].toString());
      for (int i = 1; i < operands.length; i++) {
        sum = sum.add(new BigDecimal(operands[i].toString()));
      }

      BigDecimal count = new BigDecimal(operands.length);
      BigDecimal result = sum.divide(count, MAX_NUMERIC_SCALE + 1, java.math.RoundingMode.HALF_UP);

      // Always return Double for avg (preserves precision but consistent type)
      return result.doubleValue();
    }
  }
}
