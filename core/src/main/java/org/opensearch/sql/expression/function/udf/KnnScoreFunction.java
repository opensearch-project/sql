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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * {@code KNN_SCORE(field, query, MAP_VALUE_CONSTRUCTOR('name', value)...)}: the k-NN score of a
 * vector column against a query vector, higher is nearer, equal to the score the k-NN plugin
 * reports for the field's {@code space_type}. It is the {@code _score} column of the canonical k-NN
 * plan {@code Sort(fetch=k, _score DESC) <- Project(..., _score := KNN_SCORE(...)) <- [Filter] <-
 * Scan}, which {@code KnnPlanBuilder} creates; it is not resolvable by name from a query.
 *
 * <p>Arguments: the {@code knn_vector} column, the query vector as an {@code ARRAY} of {@code REAL}
 * literals, then one {@code MAP_VALUE_CONSTRUCTOR('name', value)} per option ({@code k}, {@code
 * ef_search}, {@code nprobes}), the encoding relevance functions use for their options. Returns a
 * nullable {@code REAL}, the type of the scan's {@code _score} metadata column.
 *
 * <p>The SQL plugin only runs it as an OpenSearch {@code knn} query; executing it in memory fails
 * with "knn is only supported when pushed down", the safety net relevance functions also use.
 */
public class KnnScoreFunction extends ImplementorUDF {

  /** The operator's name; backends that resolve calls by upper-cased name look for it. */
  public static final String NAME = "KNN_SCORE";

  public KnnScoreFunction() {
    super(new KnnScoreImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(SqlTypeName.REAL).andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  /** The vector column, the query vector, then up to three options. */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(
            List.of(
                SqlTypeFamily.ARRAY,
                SqlTypeFamily.ARRAY,
                SqlTypeFamily.MAP,
                SqlTypeFamily.MAP,
                SqlTypeFamily.MAP),
            i -> i >= 2));
  }

  /** Fails: the score is only computed by the k-NN plugin, once the call is pushed down. */
  public static class KnnScoreImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      throw KnnErrors.notPushedDown();
    }
  }
}
