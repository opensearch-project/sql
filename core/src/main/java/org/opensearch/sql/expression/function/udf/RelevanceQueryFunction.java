/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
   * Starting from the 3rd parameter, they are optional parameters for relevance queries.
   * Different query has different parameter set, which will be validated in dedicated query builder
   */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(
                    ImmutableList.of(
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.STRING),
                    i -> i > 1 && i < 14) // Parameters 3-14 are optional
                .or(
                    OperandTypes.family(
                        ImmutableList.of(
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING,
                            SqlTypeFamily.STRING),
                        i -> i > 1 && i < 25))); // Parameters 3-25 are optional
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
