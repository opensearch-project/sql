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
import org.jspecify.annotations.NonNull;
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

  /*
   * The first parameter is always required (either fields or query).
   * The second parameter is query when fields are present, otherwise it's the first parameter.
   * Starting from the 3rd parameter (or 2nd when no fields), they are optional parameters for relevance queries.
   * Different query has different parameter set, which will be validated in dedicated query builder.
   * Query parameter is always required and cannot be null.
   */
  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
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
