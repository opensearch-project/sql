/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.StringUtils;

/** Lucene query that builds wildcard query. */
public class WildcardQuery extends SingleFieldQuery<WildcardQueryBuilder> {
  /**
   * Default constructor for WildcardQuery configures how RelevanceQuery.build() handles named
   * arguments.
   */
  public WildcardQuery() {
    super(FunctionParameterRepository.WildcardQueryBuildActions);
  }

  @Override
  protected String getQueryName() {
    return WildcardQueryBuilder.NAME;
  }

  @Override
  protected WildcardQueryBuilder createBuilder(String field, String query) {
    String matchText = StringUtils.convertSqlWildcardToLucene(query);
    return QueryBuilders.wildcardQuery(field, matchText);
  }
}
