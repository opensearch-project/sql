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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class RelevanceQueryFunction extends ImplementorUDF {

  public RelevanceQueryFunction() {
    super(new RelevanceQueryImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN;
  }

  /**
   * Provide operand metadata for the relevance query UDF.
   *
   * <p>The function requires at least one operand and accepts up to 25 operands total.
   * The first operand is always required (either fields or the query). Operands 2–25 are
   * optional and are expected to be map-typed parameters used by the relevance query
   * builders. The query argument itself is required and must not be null.
   *
   * @return a UDFOperandMetadata indicating between 1 and 25 operands where operands 2–25
   *         are map-typed and optional
   */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.repeat(
            SqlOperandCountRanges.between(1, 25),
            OperandTypes.MAP)); // Parameters 2-25 are optional
  }

  public static class RelevanceQueryImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      throw new UnsupportedOperationException(
          "Relevance search query functions are only supported when they are pushed down");
    }
  }
}