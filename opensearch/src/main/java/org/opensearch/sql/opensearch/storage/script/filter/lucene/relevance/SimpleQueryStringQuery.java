/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;

public class SimpleQueryStringQuery extends MultiFieldQuery<SimpleQueryStringBuilder> {
  /**
   *  Default constructor for SimpleQueryString configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public SimpleQueryStringQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<SimpleQueryStringBuilder>>builder()
        .put("analyze_wildcard", (b, v) -> b.analyzeWildcard(Boolean.parseBoolean(v.stringValue())))
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("auto_generate_synonyms_phrase_query", (b, v) ->
            b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue())))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .put("default_operator", (b, v) -> b.defaultOperator(Operator.fromString(v.stringValue())))
        .put("flags", (b, v) -> b.flags(Arrays.stream(valueOfToUpper(v).split("\\|"))
            .map(SimpleQueryStringFlag::valueOf)
            .toArray(SimpleQueryStringFlag[]::new)))
        .put("fuzzy_max_expansions", (b, v) ->
            b.fuzzyMaxExpansions(Integer.parseInt(v.stringValue())))
        .put("fuzzy_prefix_length", (b, v) ->
            b.fuzzyPrefixLength(Integer.parseInt(v.stringValue())))
        .put("fuzzy_transpositions", (b, v) ->
            b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue())))
        .put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue())))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v.stringValue()))
        .build());
  }

  @Override
  protected SimpleQueryStringBuilder createBuilder(ImmutableMap<String, Float> fields,
                                                   String query) {
    return QueryBuilders.simpleQueryStringQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return SimpleQueryStringBuilder.NAME;
  }
}
