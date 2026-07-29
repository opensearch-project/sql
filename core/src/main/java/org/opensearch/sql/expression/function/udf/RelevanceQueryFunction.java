/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
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
   * The first parameter is always required (either fields or query).
   * The second parameter is query when fields are present, otherwise it's the first parameter.
   * Starting from the 3rd parameter (or 2nd when no fields), they are optional parameters for relevance queries.
   * Different query has different parameter set, which will be validated in dedicated query builder.
   * Query parameter is always required and cannot be null.
   */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(
                    ImmutableList.of(
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP,
                        SqlTypeFamily.MAP),
                    i -> i > 0 && i < 14) // Parameters 3-14 are optional
                .or(
                    OperandTypes.family(
                        ImmutableList.of(
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP,
                            SqlTypeFamily.MAP),
                        i -> i > 0 && i < 25))); // Parameters 3-25 are optional
  }

  public static class RelevanceQueryImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // Reaching code generation means the call was not rewritten into an OpenSearch query. There
      // is no row-by-row implementation to fall back to, so report why rather than just that.
      throw new UnsupportedOperationException(
          String.format(
              Locale.ROOT,
              "Relevance search function [%s] could not be pushed down to OpenSearch, and it has no"
                  + " other execution path. Apply it directly to an indexed field before any"
                  + " command that transforms rows (eval, parse, rex, stats, top, rare, sort, head,"
                  + " lookup), and not to a column computed by the query itself. Expression: %s",
              call.getOperator().getName().toLowerCase(Locale.ROOT),
              call));
    }
  }
}
