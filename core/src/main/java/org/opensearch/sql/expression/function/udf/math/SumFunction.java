package org.opensearch.sql.expression.function.udf.math;

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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Variadic SUM function for row-wise addition of numeric values in eval/where contexts.
 *
 * <p>This function enables calculations like: - eval total = sum(1, fieldA, fieldB) -> row-wise
 * addition: 1 + fieldA + fieldB - eval total = sum(2, 3.0) -> auto-casting: 2.0 + 3.0 = 5.0
 * (DOUBLE) - where sum(score1, score2, bonus) > 80
 *
 * <p>Key behaviors: - Auto-casting: sum(2, 3.0) automatically promotes to DOUBLE arithmetic - Null
 * handling: If ANY argument is null, result is null (NullPolicy.ANY) - Type inference: Returns
 * LEAST_RESTRICTIVE type (widest numeric type among args)
 *
 * <p>Note: This function only handles scalar numeric values. Array fields are not supported.
 *
 * <p>Different from aggregation sum() which operates across rows: - stats sum(fieldA) by host ->
 * aggregation across multiple rows
 */
public class SumFunction extends ImplementorUDF {

  public SumFunction() {
    // NullPolicy.ANY: If any argument is null, result is null
    // This allows NotNullImplementor to assume all args are non-null
    super(new SumImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // LEAST_RESTRICTIVE finds the "widest" type among arguments
    // Examples: sum(1, 2.5) → DOUBLE, sum(1L, 2) → LONG
    // This enables automatic type promotion (auto-casting)
    // Apply TO_NULLABLE transform to handle null operands correctly
    return ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE);
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
  public static class SumImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.isEmpty()) {
        // sum() with no arguments should throw an exception
        throw new IllegalArgumentException("SUM function requires at least one argument");
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
            SumImplementor.class,
            "bigDecimalSum",
            Expressions.newArrayInit(Number.class, operands));
      } else {
        // For all other types, use direct expression addition
        // This generates: operand1 + operand2 + operand3
        Expression result = translatedOperands.get(0);
        for (int i = 1; i < translatedOperands.size(); i++) {
          result = Expressions.add(result, translatedOperands.get(i));
        }
        return result;
      }
    }

    public static Number bigDecimalSum(Number[] operands) {
      // Use BigDecimal for high precision calculation (called when BigDecimal detected at compile
      // time)
      BigDecimal sum = new BigDecimal(operands[0].toString());
      for (int i = 1; i < operands.length; i++) {
        sum = sum.add(new BigDecimal(operands[i].toString()));
      }
      return sum; // Return BigDecimal to preserve precision
    }
  }
}
