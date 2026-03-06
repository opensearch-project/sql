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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
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
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // EXPR_IP is mapped to SqlTypeFamily.OTHER in
    // UserDefinedFunctionUtils.convertRelDataTypeToSqlTypeName
    // We use a specific type checker to serve
    return UDFOperandMetadata.wrap(
        OperandTypes.CHARACTER_CHARACTER.or(
            new SqlOperandTypeChecker() {
              @Override
              public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
                if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
                  return false;
                }
                List<RelDataType> types = callBinding.collectOperandTypes();
                return OpenSearchTypeUtil.isIp(types.get(0), true)
                    && OpenSearchTypeUtil.isCharacter(types.get(1));
              }

              @Override
              public SqlOperandCountRange getOperandCountRange() {
                return SqlOperandCountRanges.of(2);
              }

              @Override
              public String getAllowedSignatures(SqlOperator op, String opName) {
                return "CIDRMATCH(<IP>, <STRING>)";
              }
            }));
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
