/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Base class to represent builder class for relevance queries like match_query, match_bool_prefix,
 * and match_phrase that search in a single field only.
 *
 * @param <T> The builder class for the OpenSearch query class.
 */
abstract class SingleFieldQuery<T extends QueryBuilder> extends RelevanceQuery<T> {
  public SingleFieldQuery(Map<String, QueryBuilderStep<T>> queryBuildActions) {
    super(queryBuildActions);
  }

  @Override
  protected T createQueryBuilder(List<NamedArgumentExpression> arguments) {
    // Extract 'field' and 'query'
    var field =
        arguments.stream()
            .filter(a -> a.getArgName().equalsIgnoreCase("field"))
            .findFirst()
            .orElseThrow(() -> new SemanticCheckException("'field' parameter is missing."));

    var query =
        arguments.stream()
            .filter(a -> a.getArgName().equalsIgnoreCase("query"))
            .findFirst()
            .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

    return createBuilder(
        ((ReferenceExpression) field.getValue()).getAttr(),
        query.getValue().valueOf().stringValue());
  }

  protected abstract T createBuilder(String field, String query);

  /**
   * Build single field relevance query builder based on Calcite function's operands. For
   * SingleFieldQuery, field and query string parameter are required.
   *
   * @param field Target field name
   * @param query String query to search
   * @param optionalArguments Map contains optional relevance query argument key value pairs
   * @return Final QueryBuilder
   */
  public T build(String field, String query, Map<String, String> optionalArguments) {
    T queryBuilder = createBuilder(field, query);
    return applyArguments(queryBuilder, optionalArguments);
  }
}
