/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import inet.ipaddr.IPAddress;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.utils.IPUtils;

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
    return new UDFOperandMetadata.IPOperandMetadata();
  }

  public static class CompareImplementor implements NotNullImplementor {
    private final ComparisonType comparisonType;

    public CompareImplementor(ComparisonType comparisonType) {
      this.comparisonType = comparisonType;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          CompareImplementor.class,
          "compare",
          translatedOperands.get(0),
          translatedOperands.get(1),
          Expressions.constant(comparisonType));
    }

    public static boolean compare(Object obj1, Object obj2, ComparisonType comparisonType) {
      try {
        String ip1 = extractIpString(obj1);
        String ip2 = extractIpString(obj2);
        IPAddress addr1 = IPUtils.toAddress(ip1);
        IPAddress addr2 = IPUtils.toAddress(ip2);
        int result = IPUtils.compare(addr1, addr2);
        return switch (comparisonType) {
          case EQUALS -> result == 0;
          case NOT_EQUALS -> result != 0;
          case LESS -> result < 0;
          case LESS_OR_EQUAL -> result <= 0;
          case GREATER -> result > 0;
          case GREATER_OR_EQUAL -> result >= 0;
        };
      } catch (Exception e) {
        return false;
      }
    }

    private static String extractIpString(Object obj) {
      if (obj instanceof String) return (String) obj;
      if (obj instanceof ExprIpValue) return ((ExprIpValue) obj).value();
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
