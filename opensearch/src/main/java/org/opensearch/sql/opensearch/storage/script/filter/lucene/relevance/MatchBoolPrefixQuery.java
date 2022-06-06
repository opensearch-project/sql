/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchBoolPrefixQueryBuilder from a FunctionExpression.
 */
public class MatchBoolPrefixQuery
    extends RelevanceQuery<MatchBoolPrefixQueryBuilder> {
  /**
   * Constructor for MatchBoolPrefixQuery to configure RelevanceQuery
   * with support of optional parameters.
   */
  public MatchBoolPrefixQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchBoolPrefixQueryBuilder>>builder()
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("fuzziness", (b, v) -> b.fuzziness(v.stringValue()))
        .put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v.stringValue())))
        .put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue())))
        .put("fuzzy_transpositions",
            (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue())))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v.stringValue()))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .build());
  }

  /**
   * Maps correct query builder function to class.
   * @param field  Field to execute query in
   * @param query  Text used to search field
   * @return  Object of executed query
   */
  @Override
  protected MatchBoolPrefixQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.matchBoolPrefixQuery(field, query);
  }
}
