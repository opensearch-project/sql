/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

import org.opensearch.common.Strings;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

import static org.opensearch.index.query.SimpleQueryStringFlag.ALL;
import static org.opensearch.index.query.SimpleQueryStringFlag.NONE;

public class SimpleQueryStringQuery extends RelevanceQuery<SimpleQueryStringBuilder> {
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
        .put("flags", (b, v) -> b.flags(Arrays.stream(v.stringValue().split("\\|"))
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
  public QueryBuilder build(FunctionExpression func) {
    if (func.getArguments().size() < 2) {
      throw new SemanticCheckException("'simple_query_string' must have at least two arguments");
    }
    Iterator<Expression> iterator = func.getArguments().iterator();
    var fields = (NamedArgumentExpression) iterator.next();
    var query = (NamedArgumentExpression) iterator.next();
    // Fields is a map already, but we need to convert types.
    var fieldsAndWeights = fields
        .getValue()
        .valueOf(null)
        .tupleValue()
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> e.getValue().floatValue()));

    SimpleQueryStringBuilder queryBuilder = createQueryBuilder(null,
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
  protected SimpleQueryStringBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.simpleQueryStringQuery(query);
  }
}
