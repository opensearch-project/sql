/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprIpValue;
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
public class IPCastFunction extends ImplementorUDF {

  /** Constructor for IPCastFunction. */
  public IPCastFunction() {
    super(new CastImplementor(), NullPolicy.ANY);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // This allows STRING or IP as input
    return new UDFOperandMetadata.IPCastOperandMetadata();
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Always return IP type
    return ReturnTypes.explicit(
        OpenSearchTypeFactory.TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_IP, true));
  }

  /** Implementor for IP casting. */
  public static class CastImplementor
      implements org.apache.calcite.adapter.enumerable.NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (call.getOperands().size() != 1) {
        throw new IllegalArgumentException("IP function requires exactly one operand");
      }
      if (call.getOperands().getFirst().getType() instanceof ExprIPType) {
        return translatedOperands.getFirst();
      }
      return Expressions.new_(ExprIpValue.class, translatedOperands);
    }
  }
}
