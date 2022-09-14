/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Objects;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 * Class for Lucene query that builds the query_string query.
 */
public class QueryStringQuery extends MultiFieldQuery<QueryStringQueryBuilder> {
  /**
   *  Default constructor for QueryString configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public QueryStringQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<QueryStringQueryBuilder>>builder()
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("allow_leading_wildcard", (b, v) ->
            b.allowLeadingWildcard(Boolean.parseBoolean(v.stringValue())))
        .put("analyze_wildcard", (b, v) ->
            b.analyzeWildcard(Boolean.parseBoolean(v.stringValue())))
        .put("auto_generate_synonyms_phrase_query", (b, v) ->
            b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue())))
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .put("default_operator", (b, v) ->
            b.defaultOperator(Operator.fromString(v.stringValue())))
        .put("enable_position_increments", (b, v) ->
            b.enablePositionIncrements(Boolean.parseBoolean(v.stringValue())))
        .put("fuzziness", (b, v) -> b.fuzziness(Fuzziness.build(v.stringValue())))
        .put("fuzzy_rewrite", (b, v) -> b.fuzzyRewrite(v.stringValue()))
        .put("escape", (b, v) -> b.escape(Boolean.parseBoolean(v.stringValue())))
        .put("fuzzy_max_expansions", (b, v) ->
            b.fuzzyMaxExpansions(Integer.parseInt(v.stringValue())))
        .put("fuzzy_prefix_length", (b, v) ->
            b.fuzzyPrefixLength(Integer.parseInt(v.stringValue())))
        .put("fuzzy_transpositions", (b, v) ->
            b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue())))
        .put("lenient", (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue())))
        .put("max_determinized_states", (b, v) ->
            b.maxDeterminizedStates(Integer.parseInt(v.stringValue())))
        .put("minimum_should_match", (b, v) -> b.minimumShouldMatch(v.stringValue()))
        .put("quote_analyzer", (b, v) -> b.quoteAnalyzer(v.stringValue()))
        .put("phrase_slop", (b, v) -> b.phraseSlop(Integer.parseInt(v.stringValue())))
        .put("quote_field_suffix", (b, v) -> b.quoteFieldSuffix(v.stringValue()))
        .put("rewrite", (b, v) -> b.rewrite(v.stringValue()))
        .put("type", (b, v) -> b.type(MultiMatchQueryBuilder.Type.parse(valueOfToLower(v),
            LoggingDeprecationHandler.INSTANCE)))
        .put("tie_breaker", (b, v) -> b.tieBreaker(Float.parseFloat(v.stringValue())))
        .put("time_zone", (b, v) -> b.timeZone(v.stringValue()))
        .build());
  }


  /**
   * Builds QueryBuilder with query value and other default parameter values set.
   *
   * @param fields : A map of field names and their boost values
   * @param query : Query value for query_string query
   * @return : Builder for query_string query
   */
  @Override
  protected QueryStringQueryBuilder createBuilder(ImmutableMap<String, Float> fields,
                                                  String query) {
    return QueryBuilders.queryStringQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return QueryStringQueryBuilder.NAME;
  }
}
