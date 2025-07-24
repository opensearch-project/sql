/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * {@code compare(value1, value2)} compares two values. It extends Calcite's built-in comparators by
 * supporting IP comparison.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(COMPARABLE, COMPARABLE) -> BOOLEAN
 * </ul>
 */
public class CompareFunctions extends ImplementorUDF {

  private CompareFunctions(ComparisonType comparisonType) {
    super(new CompareImplementor(comparisonType), NullPolicy.ANY);
  }

  public static CompareFunctions less() {
    return new CompareFunctions(ComparisonType.LESS);
  }

  public static CompareFunctions greater() {
    return new CompareFunctions(ComparisonType.GREATER);
  }

  public static CompareFunctions lessOrEquals() {
    return new CompareFunctions(ComparisonType.LESS_OR_EQUAL);
  }

  public static CompareFunctions greaterOrEquals() {
    return new CompareFunctions(ComparisonType.GREATER_OR_EQUAL);
  }

  public static CompareFunctions equals() {
    return new CompareFunctions(ComparisonType.EQUALS);
  }

  public static CompareFunctions notEquals() {
    return new CompareFunctions(ComparisonType.NOT_EQUALS);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Its type checker will be assigned according to the function name.
    return null;
  }

  public static class CompareImplementor implements NotNullImplementor {
    private final ComparisonType comparisonType;

    public CompareImplementor(ComparisonType comparisonType) {
      this.comparisonType = comparisonType;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      if (!containsIpOperands(call)) {
        // Call built-in compare function for non-IP operands
        ExpressionType expressionType =
            switch (comparisonType) {
              case EQUALS -> ExpressionType.Equal;
              case NOT_EQUALS -> ExpressionType.NotEqual;
              case LESS -> ExpressionType.LessThan;
              case LESS_OR_EQUAL -> ExpressionType.LessThanOrEqual;
              case GREATER -> ExpressionType.GreaterThan;
              case GREATER_OR_EQUAL -> ExpressionType.GreaterThanOrEqual;
            };
        return Expressions.makeBinary(
            expressionType, translatedOperands.get(0), translatedOperands.get(1));
      }

      Expression compareResult =
          Expressions.call(
              CompareImplementor.class,
              "compareTo",
              translatedOperands.get(0),
              translatedOperands.get(1));

      return generateComparisonExpression(compareResult, comparisonType);
    }

    private static boolean containsIpOperands(RexCall call) {
      var left = call.getOperands().get(0);
      var right = call.getOperands().get(1);
      var leftType = OpenSearchTypeFactory.convertRelDataTypeToExprType(left.getType());
      var rightType = OpenSearchTypeFactory.convertRelDataTypeToExprType(right.getType());
      return leftType == ExprCoreType.IP || rightType == ExprCoreType.IP;
    }

    private static Expression generateComparisonExpression(
        Expression compareResult, ComparisonType comparisonType) {
      return switch (comparisonType) {
        case EQUALS -> Expressions.equal(compareResult, Expressions.constant(0));
        case NOT_EQUALS -> Expressions.notEqual(compareResult, Expressions.constant(0));
        case LESS -> Expressions.lessThan(compareResult, Expressions.constant(0));
        case LESS_OR_EQUAL -> Expressions.lessThanOrEqual(compareResult, Expressions.constant(0));
        case GREATER -> Expressions.greaterThan(compareResult, Expressions.constant(0));
        case GREATER_OR_EQUAL -> Expressions.greaterThanOrEqual(
            compareResult, Expressions.constant(0));
      };
    }

    public static int compareTo(Object obj1, Object obj2) {
      ExprIpValue v1 = toExprIpValue(obj1);
      ExprIpValue v2 = toExprIpValue(obj2);
      return v1.compare(v2);
    }

    private static ExprIpValue toExprIpValue(Object obj) {
      if (obj instanceof ExprIpValue) {
        return (ExprIpValue) obj;
      } else if (obj instanceof String) {
        return new ExprIpValue((String) obj);
      }
      throw new IllegalArgumentException("Invalid IP type: " + obj);
    }
  }

  public enum ComparisonType {
    EQUALS,
    NOT_EQUALS,
    LESS,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL
  }
}
