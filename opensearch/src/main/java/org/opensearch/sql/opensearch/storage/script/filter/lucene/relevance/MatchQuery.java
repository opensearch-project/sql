/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchQueryBuilder from a FunctionExpression.
 */
public class MatchQuery extends SingleFieldQuery<MatchQueryBuilder> {
  /**
   *  Default constructor for MatchQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchQuery() {
    super(FunctionParameterRepository.MatchQueryBuildActions);
  }

  @Override
  protected MatchQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchQueryBuilder.NAME;
  }
}
