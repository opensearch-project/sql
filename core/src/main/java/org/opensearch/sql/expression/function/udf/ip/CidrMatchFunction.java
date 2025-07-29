/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.expression.ip.IPFunctions;

/**
 * {@code cidrmatch(ip, cidr)} checks if ip is within the specified cidr range.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING, STRING) -> BOOLEAN
 *   <li>(IP, STRING) -> BOOLEAN
 * </ul>
 */
public class CidrMatchFunction extends ImplementorUDF {
  public CidrMatchFunction() {
    super(new CidrMatchImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // EXPR_IP is mapped to SqlTypeFamily.OTHER in
    // UserDefinedFunctionUtils.convertRelDataTypeToSqlTypeName
    // We use a specific type checker to serve
    return UDFOperandMetadata.wrapUDT(
        List.of(
            List.of(ExprCoreType.IP, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.STRING)));
  }

  public static class CidrMatchImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(CidrMatchImplementor.class, "cidrMatch", translatedOperands);
    }

    public static boolean cidrMatch(Object ip, String cidr) {
      ExprValue ipValue;
      if (ip instanceof ExprIpValue) {
        ipValue = (ExprIpValue) ip;
      } else {
        // Deserialization workaround
        ipValue = new ExprIpValue((String) ip);
      }
      ExprValue cidrValue = ExprValueUtils.stringValue(cidr);
      return (boolean) IPFunctions.exprCidrMatch(ipValue, cidrValue).valueForCalcite();
    }

    public static boolean cidrMatch(String ip, String cidr) {
      ExprIpValue ipValue = (ExprIpValue) ExprValueUtils.ipValue(ip);
      return cidrMatch(ipValue, cidr);
    }
  }
}
