/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/**
 * Base class for query abstraction that builds a relevance query from function expression.
 */
public abstract class RelevanceQuery<T extends QueryBuilder> extends LuceneQuery {

  protected Map<String, QueryBuilderStep<T>>
      queryBuildActions;

  public RelevanceQuery() {
    super();
    this.queryBuildActions = buildActionMap();
  }

  @Override
  public QueryBuilder build(FunctionExpression func) {
    Iterator<Expression> iterator = func.getArguments().iterator();
    NamedArgumentExpression field = (NamedArgumentExpression) iterator.next();
    NamedArgumentExpression query = (NamedArgumentExpression) iterator.next();
    T queryBuilder = createQueryBuilder(field.getValue().valueOf(null).stringValue(),
        query.getValue().valueOf(null).stringValue());
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
      if (!queryBuildActions.containsKey(arg.getArgName())) {
        throw new SemanticCheckException(String
            .format("Parameter %s is invalid for match function.", arg.getArgName()));
      }
      (queryBuildActions
          .get(arg.getArgName()))
          .apply(queryBuilder, arg.getValue().valueOf(null));
    }
    return queryBuilder;
  }

  protected abstract T createQueryBuilder(String field, String query);

  protected abstract Map<String, QueryBuilderStep<T>> buildActionMap();

  /**
   * Convenience interface for a function that updates a QueryBuilder
   * based on ExprValue.
   * @param <T> Concrete query builder
   */
  public interface QueryBuilderStep<T extends QueryBuilder> extends
      BiFunction<T, ExprValue, T> {

  }
}
