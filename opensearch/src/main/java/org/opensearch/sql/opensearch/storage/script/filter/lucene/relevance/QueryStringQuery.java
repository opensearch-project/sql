/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Objects;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 * REMEMBER YOUR JAVADOCS.
 */
public class QueryStringQuery extends RelevanceQuery<QueryStringQueryBuilder> {
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
        .put("time_zone", (b, v) -> b.timeZone(v.stringValue()))
        .build());
  }

  @Override
  public QueryBuilder build(FunctionExpression func) {
    Iterator<Expression> iterator = func.getArguments().iterator();
    if (func.getArguments().size() < 2) {
      throw new SemanticCheckException("'query_string' must have at least two arguments");
    }
    NamedArgumentExpression fields = (NamedArgumentExpression) iterator.next();
    NamedArgumentExpression query = (NamedArgumentExpression) iterator.next();
    // Fields is a map already, but we need to convert types.
    var fieldsAndWeights = fields
        .getValue()
        .valueOf(null)
        .tupleValue()
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> e.getValue().floatValue()));

    QueryStringQueryBuilder queryBuilder = createQueryBuilder(null,
        query.getValue().valueOf(null).stringValue())
        .fields(fieldsAndWeights);
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!queryBuildActions.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(
            String.format("Parameter %s is invalid for %s function.",
                arg.getArgName(), queryBuilder.getWriteableName()));
      }
      (Objects.requireNonNull(
          queryBuildActions
              .get(arg.getArgName())))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }


  @Override
  protected QueryStringQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.queryStringQuery(query);
  }
}
