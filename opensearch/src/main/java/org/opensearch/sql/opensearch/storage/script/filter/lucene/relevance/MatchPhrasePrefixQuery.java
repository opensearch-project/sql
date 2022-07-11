/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
    * Lucene query that builds a match_phrase_prefix query.
    */
public class MatchPhrasePrefixQuery  extends RelevanceQuery<MatchPhrasePrefixQueryBuilder> {
  /**
   *  Default constructor for MatchPhrasePrefixQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchPhrasePrefixQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchPhrasePrefixQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("slop", (b, v) -> b.slop(Integer.parseInt(v.stringValue())))
        .put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue())))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue())))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .build());
  }

  @Override
  protected MatchPhrasePrefixQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.matchPhrasePrefixQuery(field, query);
  }
}
