/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.List;
import java.util.Locale;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Function that casts values to IP type.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING) -> IP
 *   <li>(IP) -> IP
 * </ul>
 */
public class IPFunction extends ImplementorUDF {

  public IPFunction() {
    super(new CastImplementor(), NullPolicy.ANY);
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        new SqlOperandTypeChecker() {
          @Override
          public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            RelDataType type = callBinding.getOperandType(0);
            boolean valid = OpenSearchTypeUtil.isIp(type) || OpenSearchTypeUtil.isCharacter(type);
            if (!valid && throwOnFailure) {
              throw callBinding.newValidationSignatureError();
            }
            return valid;
          }

          @Override
          public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(1);
          }

          @Override
          public String getAllowedSignatures(SqlOperator op, String opName) {
            return "IP(<IP>), IP(<STRING>)";
          }
        });
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.IP_FORCE_NULLABLE;
  }

  public static class CastImplementor
      implements org.apache.calcite.adapter.enumerable.NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (call.getOperands().size() != 1) {
        throw new IllegalArgumentException("IP function requires exactly one operand");
      }
      ExprType argType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              call.getOperands().getFirst().getType());
      if (argType == ExprCoreType.IP) {
        return translatedOperands.getFirst();
      } else if (argType == ExprCoreType.STRING) {
        return Expressions.new_(ExprIpValue.class, translatedOperands);
      } else {
        throw new ExpressionEvaluationException(
            String.format(
                Locale.ROOT,
                "Cannot convert %s to IP, only STRING and IP types are supported",
                argType));
      }
    }
  }
}
