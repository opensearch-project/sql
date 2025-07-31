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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
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
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrapUDT(
        List.of(List.of(ExprCoreType.IP), List.of(ExprCoreType.STRING)));
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        OpenSearchTypeFactory.TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP, true));
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
              call.getOperands().get(0).getType());
      if (argType == ExprCoreType.IP) {
        return translatedOperands.get(0);
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
