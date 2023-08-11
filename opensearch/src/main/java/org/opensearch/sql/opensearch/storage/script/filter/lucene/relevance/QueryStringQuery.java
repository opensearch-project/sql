/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;

/** Class for Lucene query that builds the query_string query. */
public class QueryStringQuery extends MultiFieldQuery<QueryStringQueryBuilder> {
  /**
   * Default constructor for QueryString configures how RelevanceQuery.build() handles named
   * arguments.
   */
  public QueryStringQuery() {
    super(FunctionParameterRepository.QueryStringQueryBuildActions);
  }

  /**
   * Builds QueryBuilder with query value and other default parameter values set.
   *
   * @param fields : A map of field names and their boost values
   * @param query : Query value for query_string query
   * @return : Builder for query_string query
   */
  @Override
  protected QueryStringQueryBuilder createBuilder(
      ImmutableMap<String, Float> fields, String query) {
    return QueryBuilders.queryStringQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return QueryStringQueryBuilder.NAME;
  }
}
