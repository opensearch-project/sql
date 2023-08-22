/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/** Lucene query that builds a match_phrase_prefix query. */
public class MatchPhrasePrefixQuery extends SingleFieldQuery<MatchPhrasePrefixQueryBuilder> {
  /**
   * Default constructor for MatchPhrasePrefixQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchPhrasePrefixQuery() {
    super(FunctionParameterRepository.MatchPhrasePrefixQueryBuildActions);
  }

  @Override
  protected MatchPhrasePrefixQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchPhrasePrefixQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchPhrasePrefixQueryBuilder.NAME;
  }
}
