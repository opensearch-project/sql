/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * {@code compare(ip1, ip2)} compares two IP addresses using a provided op.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(IP, STRING) -> BOOLEAN
 *   <li>(STRING, IP) -> BOOLEAN
 *   <li>(IP, IP) -> BOOLEAN
 * </ul>
 */
public class CompareIpFunction extends ImplementorUDF {

  private CompareIpFunction(ComparisonType comparisonType) {
    super(new CompareImplementor(comparisonType), NullPolicy.ANY);
  }

  public static CompareIpFunction less() {
    return new CompareIpFunction(ComparisonType.LESS);
  }

  public static CompareIpFunction greater() {
    return new CompareIpFunction(ComparisonType.GREATER);
  }

  public static CompareIpFunction lessOrEquals() {
    return new CompareIpFunction(ComparisonType.LESS_OR_EQUAL);
  }

  public static CompareIpFunction greaterOrEquals() {
    return new CompareIpFunction(ComparisonType.GREATER_OR_EQUAL);
  }

  public static CompareIpFunction equals() {
    return new CompareIpFunction(ComparisonType.EQUALS);
  }

  public static CompareIpFunction notEquals() {
    return new CompareIpFunction(ComparisonType.NOT_EQUALS);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrapUDT(List.of(List.of(ExprCoreType.IP, ExprCoreType.IP)));
  }

  public static class CompareImplementor implements NotNullImplementor {
    private final ComparisonType comparisonType;

    public CompareImplementor(ComparisonType comparisonType) {
      this.comparisonType = comparisonType;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression compareResult =
          Expressions.call(
              translatedOperands.get(0),
              "compare",
              Expressions.convert_(translatedOperands.get(1), ExprValue.class));

      return generateComparisonExpression(compareResult, comparisonType);
    }

    private static Expression generateComparisonExpression(
        Expression compareResult, ComparisonType comparisonType) {
      final ConstantExpression zero = Expressions.constant(0);
      return switch (comparisonType) {
        case EQUALS -> Expressions.equal(compareResult, zero);
        case NOT_EQUALS -> Expressions.notEqual(compareResult, zero);
        case LESS -> Expressions.lessThan(compareResult, zero);
        case LESS_OR_EQUAL -> Expressions.lessThanOrEqual(compareResult, zero);
        case GREATER -> Expressions.greaterThan(compareResult, zero);
        case GREATER_OR_EQUAL -> Expressions.greaterThanOrEqual(compareResult, zero);
      };
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
