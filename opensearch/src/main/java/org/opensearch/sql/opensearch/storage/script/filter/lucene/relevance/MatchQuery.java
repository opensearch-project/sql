/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.function.BiFunction;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

public class MatchQuery extends LuceneQuery {
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> analyzer =
      (b, v) -> b.analyzer(v.stringValue());
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> synonymsPhrase =
      (b, v) -> b.autoGenerateSynonymsPhraseQuery(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> fuzziness =
      (b, v) -> b.fuzziness(v.stringValue());
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> maxExpansions =
      (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> prefixLength =
      (b, v) -> b.prefixLength(Integer.parseInt(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> fuzzyTranspositions =
      (b, v) -> b.fuzzyTranspositions(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> fuzzyRewrite =
      (b, v) -> b.fuzzyRewrite(v.stringValue());
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> lenient =
      (b, v) -> b.lenient(Boolean.parseBoolean(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> operator =
      (b, v) -> b.operator(Operator.fromString(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> minimumShouldMatch =
      (b, v) -> b.minimumShouldMatch(v.stringValue());
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> zeroTermsQuery =
      (b, v) -> b.zeroTermsQuery(
          org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue()));
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> boost =
      (b, v) -> b.boost(Float.parseFloat(v.stringValue()));

  ImmutableMap<Object, Object> argAction = ImmutableMap.builder()
      .put("analyzer", analyzer)
      .put("auto_generate_synonyms_phrase_query", synonymsPhrase)
      .put("fuzziness", fuzziness)
      .put("max_expansions", maxExpansions)
      .put("prefix_length", prefixLength)
      .put("fuzzy_transpositions", fuzzyTranspositions)
      .put("fuzzy_rewrite", fuzzyRewrite)
      .put("lenient", lenient)
      .put("operator", operator)
      .put("minimum_should_match", minimumShouldMatch)
      .put("zero_terms_query", zeroTermsQuery)
      .put("boost", boost)
      .build();

  @Override
  public QueryBuilder build(FunctionExpression func) {
    if (func.getArguments().size() < 2) {
      throw new SemanticCheckException("match must have at least two arguments");
    }
    Iterator<Expression> iterator = func.getArguments().iterator();
    NamedArgumentExpression field = (NamedArgumentExpression) iterator.next();
    NamedArgumentExpression query = (NamedArgumentExpression) iterator.next();
    MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(
        field.getValue().valueOf(null).stringValue(),
        query.getValue().valueOf(null).stringValue());
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!argAction.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(String
            .format("Parameter %s is invalid for match function.", arg.getArgName()));
      }
      ((BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder>) argAction
          .get(arg.getArgName()))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }
}
