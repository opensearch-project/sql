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
import org.apache.calcite.sql.type.*;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.utils.IPUtils;

/**
 * {@code Equals(ip1, ip2)} checks if two IP addresses are equal.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING, STRING) -> BOOLEAN
 *   <li>(IP, STRING) -> BOOLEAN
 *   <li>(STRING, IP) -> BOOLEAN
 *   <li>(IP, IP) -> BOOLEAN
 * </ul>
 */
public class EqualsIpFunction extends ImplementorUDF {
  public EqualsIpFunction() {
    super(new EqualsImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  //  public UDFOperandMetadata getOperandMetadata() {
  //    return UDFOperandMetadata.wrap(
  //        (CompositeOperandTypeChecker)
  //            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NULL)
  //                .or(OperandTypes.family(SqlTypeFamily.NULL, SqlTypeFamily.STRING))
  //                .or(OperandTypes.family(SqlTypeFamily.NULL, SqlTypeFamily.NULL)));
  //  }
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.STRING_STRING);
  }

  public static class EqualsImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(EqualsImplementor.class, "Equals", translatedOperands);
    }

    public static boolean Equals(String ip1, String ip2) {
      try {
        IPAddress ipAddress1 = IPUtils.toAddress(ip1);
        IPAddress ipAddress2 = IPUtils.toAddress(ip2);
        return IPUtils.compare(ipAddress1, ipAddress2) == 0;
      } catch (SemanticCheckException e) {
        return false;
      }
    }

    public static boolean Equals(String ip1, ExprIpValue ip2) {
      String ipAddress2 = ip2.value();
      return Equals(ip1, ipAddress2);
    }

    public static boolean Equals(ExprIpValue ip1, String ip2) {
      String ipAddress1 = ip1.value();
      return Equals(ipAddress1, ip2);
    }

    public static boolean Equals(ExprIpValue ip1, ExprIpValue ip2) {
      String ipAddress1 = ip1.value();
      String ipAddress2 = ip2.value();
      return Equals(ipAddress1, ipAddress2);
    }
  }
}
