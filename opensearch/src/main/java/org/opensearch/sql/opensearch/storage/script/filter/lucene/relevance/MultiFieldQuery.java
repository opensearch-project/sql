/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
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
  public T createQueryBuilder(List<NamedArgumentExpression> arguments) {
    // Extract 'fields' and 'query'
    var fields = arguments.stream()
        .filter(a -> a.getArgName().equalsIgnoreCase("fields"))
        .findFirst()
        .orElseThrow(() -> new SemanticCheckException("'fields' parameter is missing."));

    var query = arguments.stream()
        .filter(a -> a.getArgName().equalsIgnoreCase("query"))
        .findFirst()
        .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

    var fieldsAndWeights = fields
        .getValue()
        .valueOf()
        .tupleValue()
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> e.getValue().floatValue()));

    return createBuilder(fieldsAndWeights, query.getValue().valueOf().stringValue());
  }

  protected abstract  T createBuilder(ImmutableMap<String, Float> fields, String query);
}
