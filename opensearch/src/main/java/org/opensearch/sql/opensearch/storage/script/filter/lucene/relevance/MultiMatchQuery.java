/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Objects;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

public class MultiMatchQuery extends RelevanceQuery<MultiMatchQueryBuilder> {
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
            org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue())))
        .build());
  }

  @Override
  public QueryBuilder build(FunctionExpression func) {
    if (func.getArguments().size() < 2) {
      throw new SemanticCheckException("'multi_match' must have at least two arguments");
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

    MultiMatchQueryBuilder queryBuilder = createQueryBuilder(null,
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
  protected MultiMatchQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.multiMatchQuery(query);
  }
}
