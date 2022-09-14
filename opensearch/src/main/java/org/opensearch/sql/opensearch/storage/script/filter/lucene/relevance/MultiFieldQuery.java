/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 *  Base class to represent relevance queries that search multiple fields.
 * @param <T> The builder class for the OpenSearch query.
 */
abstract class MultiFieldQuery<T extends QueryBuilder> extends RelevanceQuery<T> {

  public MultiFieldQuery(Map<String, QueryBuilderStep<T>> queryBuildActions) {
    super(queryBuildActions);
  }

  @Override
  public T createQueryBuilder(NamedArgumentExpression fields, NamedArgumentExpression queryExpr) {
    var fieldsAndWeights = fields
        .getValue()
        .valueOf(null)
        .tupleValue()
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> e.getValue().floatValue()));
    var query = queryExpr.getValue().valueOf(null).stringValue();
    return createBuilder(fieldsAndWeights, query);
  }

  protected abstract  T createBuilder(ImmutableMap<String, Float> fields, String query);
}
