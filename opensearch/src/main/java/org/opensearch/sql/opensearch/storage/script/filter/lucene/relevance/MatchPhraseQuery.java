/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/**
 * Lucene query that builds a match_phrase query.
 */
public class MatchPhraseQuery extends LuceneQuery {
  private final FluentAction analyzer =
      (b, v) -> b.analyzer(v.stringValue());
  private final FluentAction slop =
      (b, v) -> b.slop(Integer.parseInt(v.stringValue()));
  private final FluentAction
        zeroTermsQuery = (b, v) -> b.zeroTermsQuery(
               org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue()));

  interface FluentAction
      extends BiFunction<MatchPhraseQueryBuilder, ExprValue, MatchPhraseQueryBuilder>  {
  }

  ImmutableMap<Object, FluentAction>
      argAction =
      ImmutableMap.<Object, FluentAction>builder()
          .put("analyzer", analyzer)
          .put("slop", slop)
          .put("zero_terms_query", zeroTermsQuery)
          .build();

  @Override
  public QueryBuilder build(FunctionExpression func) {
    List<Expression> arguments = func.getArguments();
    if (arguments.size() < 2) {
      throw new SemanticCheckException("match_phrase requires at least two parameters");
    }
    NamedArgumentExpression field = (NamedArgumentExpression) arguments.get(0);
    NamedArgumentExpression query = (NamedArgumentExpression) arguments.get(1);
    MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(
        field.getValue().valueOf(null).stringValue(),
        query.getValue().valueOf(null).stringValue());

    Iterator<Expression> iterator = arguments.listIterator(2);
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!argAction.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(String
            .format("Parameter %s is invalid for match_phrase function.", arg.getArgName()));
      }
      (Objects.requireNonNull(
          argAction
              .get(arg.getArgName())))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }
}
