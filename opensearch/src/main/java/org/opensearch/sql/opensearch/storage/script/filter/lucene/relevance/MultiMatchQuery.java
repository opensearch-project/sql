/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilders;

public class MultiMatchQuery extends MultiFieldQuery<MultiMatchQueryBuilder> {
  /**
   *  Default constructor for MultiMatch configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MultiMatchQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MultiMatchQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("auto_generate_synonyms_phrase_query", (b, v) ->
            b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue())))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .put("cutoff_frequency", (b, v) -> b.cutoffFrequency(Float.parseFloat(v.stringValue())))
        .put("fuzziness", (b, v) -> b.fuzziness(v.stringValue()))
        .put("fuzzy_transpositions", (b, v) ->
            b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue())))
        .put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue())))
        .put("max_expansions", (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue())))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("operator", (b, v) -> b.operator(Operator.fromString(v.stringValue())))
        .put("prefix_length", (b, v) -> b.prefixLength(Integer.parseInt(v.stringValue())))
        .put("tie_breaker", (b, v) -> b.tieBreaker(Float.parseFloat(v.stringValue())))
        .put("type", (b, v) -> b.type(v.stringValue()))
        .put("slop", (b, v) -> b.slop(Integer.parseInt(v.stringValue())))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(valueOfToUpper(v))))
        .build());
  }

  @Override
  protected MultiMatchQueryBuilder createBuilder(ImmutableMap<String, Float> fields, String query) {
    return QueryBuilders.multiMatchQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return MultiMatchQueryBuilder.NAME;
  }
}
