/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.function.BiFunction;

import org.opensearch.index.query.*;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

public class MatchPhraseQuery extends LuceneQuery {
  private final BiFunction<MatchPhraseQueryBuilder, ExprValue, MatchPhraseQueryBuilder> analyzer =
          (b, v) -> b.analyzer(v.stringValue());
  private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> slop =
          (b, v) -> b.maxExpansions(Integer.parseInt(v.stringValue()));
  private final BiFunction<MatchPhraseQueryBuilder, ExprValue, MatchPhraseQueryBuilder> zeroTermsQuery =
          (b, v) -> b.zeroTermsQuery(
                  org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue()));

  ImmutableMap<Object, Object> argAction = ImmutableMap.builder()
          .put("analyzer", analyzer)
          .put("slop", slop)
          .put("zero_terms_query", zeroTermsQuery)
          .build();

  @Override
  public QueryBuilder build(FunctionExpression func) {
    Iterator<Expression> iterator = func.getArguments().iterator();
    NamedArgumentExpression field = (NamedArgumentExpression) iterator.next();
    NamedArgumentExpression query = (NamedArgumentExpression) iterator.next();
    MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(
            field.getValue().valueOf(null).stringValue(),
            query.getValue().valueOf(null).stringValue());
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!argAction.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(String
                .format("Parameter %s is invalid for match function.", arg.getArgName()));
      }
      ((BiFunction<MatchPhraseQueryBuilder, ExprValue, MatchPhraseQueryBuilder>) argAction
              .get(arg.getArgName()))
              .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }
}
