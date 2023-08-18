/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/** Lucene query that builds a match_phrase query. */
public class MatchPhraseQuery extends SingleFieldQuery<MatchPhraseQueryBuilder> {
  /**
   * Default constructor for MatchPhraseQuery configures how RelevanceQuery.build() handles named
   * arguments.
   */
  public MatchPhraseQuery() {
    super(FunctionParameterRepository.MatchPhraseQueryBuildActions);
  }

  @Override
  protected MatchPhraseQueryBuilder createBuilder(String field, String query) {
    return QueryBuilders.matchPhraseQuery(field, query);
  }

  @Override
  protected String getQueryName() {
    return MatchPhraseQueryBuilder.NAME;
  }
}
