/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;

/**
 * Class for Lucene query that builds the 'query' query.
 */
public class QueryQuery extends NoFieldQuery<QueryStringQueryBuilder> {

  private final String queryQueryName = "query";

  /**
   * Default constructor for QueryQuery configures how RelevanceQuery.build() handles
   * named arguments by calling the constructor of QueryStringQuery.
   */
  public QueryQuery() {
    super(FunctionParameterRepository.QueryStringQueryBuildActions);
  }

  /**
   * Builds QueryBuilder with query value and other default parameter values set.
   *
   * @param query : Query value for query_string query
   * @return : Builder for query query
   */
  protected QueryStringQueryBuilder createBuilder(String query) {
    return QueryBuilders.queryStringQuery(query);
  }

  @Override
  public String getQueryName() {
    return queryQueryName;
  }
}
