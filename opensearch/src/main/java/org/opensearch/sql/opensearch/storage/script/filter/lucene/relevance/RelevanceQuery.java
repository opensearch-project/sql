/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/**
 * Base class for query abstraction that builds a relevance query from function expression.
 */
@RequiredArgsConstructor
public abstract class RelevanceQuery<T extends QueryBuilder> extends LuceneQuery {
  private final Map<String, QueryBuilderStep<T>> queryBuildActions;

  @Override
  public QueryBuilder build(FunctionExpression func) {
    List<Expression> arguments = func.getArguments();
    if (arguments.size() < 2) {
      throw new SyntaxCheckException(
          String.format("%s requires at least two parameters", getQueryName()));
    }
    NamedArgumentExpression field = (NamedArgumentExpression) arguments.get(0);
    NamedArgumentExpression query = (NamedArgumentExpression) arguments.get(1);
    T queryBuilder = createQueryBuilder(field, query);

    Iterator<Expression> iterator = arguments.listIterator(2);
    Set<String> visitedParms = new HashSet();
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      String argNormalized = arg.getArgName().toLowerCase();
      if (visitedParms.contains(argNormalized)) {
        throw new SemanticCheckException(String.format("Parameter '%s' can only be specified once.",
            argNormalized));
      } else {
        visitedParms.add(argNormalized);
      }

      if (!queryBuildActions.containsKey(argNormalized)) {
        throw new SemanticCheckException(
            String.format("Parameter %s is invalid for %s function.",
                argNormalized, queryBuilder.getWriteableName()));
      }
      (Objects.requireNonNull(
          queryBuildActions
              .get(argNormalized)))
          .apply(queryBuilder, arg.getValue().valueOf());
    }
    return queryBuilder;
  }

  protected abstract T createQueryBuilder(NamedArgumentExpression field,
                                          NamedArgumentExpression query);

  protected abstract String getQueryName();

  /**
   * Convenience interface for a function that updates a QueryBuilder
   * based on ExprValue.
   *
   * @param <T> Concrete query builder
   */
  protected interface QueryBuilderStep<T extends QueryBuilder> extends
      BiFunction<T, ExprValue, T> {
  }

  public static String valueOfToUpper(ExprValue v) {
    return v.stringValue().toUpperCase();
  }

  public static String valueOfToLower(ExprValue v) {
    return v.stringValue().toLowerCase();
  }
}
