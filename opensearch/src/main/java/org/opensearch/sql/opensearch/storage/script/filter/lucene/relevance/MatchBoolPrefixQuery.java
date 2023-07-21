/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchBoolPrefixQueryBuilder from a FunctionExpression.
 */
public class MatchBoolPrefixQuery
    extends SingleFieldQuery<MatchBoolPrefixQueryBuilder> {
  /**
   * Constructor for MatchBoolPrefixQuery to configure RelevanceQuery
   * with support of optional parameters.
   */
  public MatchBoolPrefixQuery() {
    super(FunctionParameterRepository.MatchBoolPrefixQueryBuildActions);
  }

  /**
   * Maps correct query builder function to class.
   * @param field  Field to execute query in
   * @param query  Text used to search field
   * @return  Object of executed query
   */
  @Override
  protected MatchBoolPrefixQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchBoolPrefixQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchBoolPrefixQueryBuilder.NAME;
  }
}
