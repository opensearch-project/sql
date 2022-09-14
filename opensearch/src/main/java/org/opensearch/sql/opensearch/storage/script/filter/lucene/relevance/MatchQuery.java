/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
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
    super(ImmutableMap.<String, QueryBuilderStep<MatchQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("auto_generate_synonyms_phrase_query",
            (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue())))
        .put("fuzziness", (b, v) -> b.fuzziness(valueOfToUpper(v)))
        .put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue())))
        .put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v.stringValue())))
        .put("fuzzy_transpositions",
            (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue())))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v.stringValue()))
        .put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue())))
        .put("operator", (b, v) -> b.operator(Operator.fromString(v.stringValue())))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(valueOfToUpper(v))))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .build());
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
