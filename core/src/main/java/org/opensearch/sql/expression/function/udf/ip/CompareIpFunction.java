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
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
  private final IpComparisonOperators.ComparisonOperator operator;

  public CompareIpFunction(IpComparisonOperators.ComparisonOperator operator) {
    super(new CompareImplementor(operator), NullPolicy.ANY);
    this.operator = operator;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NULL)
                .or(OperandTypes.family(SqlTypeFamily.NULL, SqlTypeFamily.STRING))
                .or(OperandTypes.family(SqlTypeFamily.NULL, SqlTypeFamily.NULL)));
  }

  public static class CompareImplementor implements NotNullImplementor {
    private final IpComparisonOperators.ComparisonOperator operator;

    public CompareImplementor(IpComparisonOperators.ComparisonOperator operator) {
      this.operator = operator;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          CompareImplementor.class,
          "compare",
          translatedOperands.get(0),
          translatedOperands.get(1),
          Expressions.constant(operator.name()));
    }

    public static boolean compare(Object obj1, Object obj2, String opName) {
      try {
        IpComparisonOperators.ComparisonOperator op =
            IpComparisonOperators.ComparisonOperator.valueOf(opName);
        String ip1 = extractIpString(obj1);
        String ip2 = extractIpString(obj2);
        IPAddress addr1 = IPUtils.toAddress(ip1);
        IPAddress addr2 = IPUtils.toAddress(ip2);
        int result = IPUtils.compare(addr1, addr2);

        switch (op) {
          case EQUALS:
            return result == 0;
          case NOT_EQUALS:
            return result != 0;
          case LESS:
            return result < 0;
          case LESS_OR_EQUAL:
            return result <= 0;
          case GREATER:
            return result > 0;
          case GREATER_OR_EQUAL:
            return result >= 0;
          default:
            return false;
        }
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
}
