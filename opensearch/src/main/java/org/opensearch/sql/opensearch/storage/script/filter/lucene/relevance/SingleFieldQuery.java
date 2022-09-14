/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.expression.NamedArgumentExpression;

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
  protected T createQueryBuilder(NamedArgumentExpression fields, NamedArgumentExpression query) {
    return createBuilder(
        fields.getValue().valueOf(null).stringValue(),
        query.getValue().valueOf(null).stringValue());
  }

  protected abstract T createBuilder(String field, String query);
}
