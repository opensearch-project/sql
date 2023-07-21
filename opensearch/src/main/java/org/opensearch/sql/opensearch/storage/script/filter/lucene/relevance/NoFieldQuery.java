/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/**
 * Base class to represent relevance queries that have no 'fields' array as an argument.
 *
 * @param <T> The builder class for the OpenSearch query.
 */
abstract class NoFieldQuery<T extends QueryBuilder> extends RelevanceQuery<T> {
  public NoFieldQuery(Map<String, QueryBuilderStep<T>> queryBuildActions) {
    super(queryBuildActions);
  }

  @Override
  protected void ignoreArguments(List<NamedArgumentExpression> arguments) {
    arguments.removeIf(a -> a.getArgName().equalsIgnoreCase("query"));
  }

  @Override
  protected void checkValidArguments(String argNormalized, T queryBuilder) {
    if (!getQueryBuildActions().containsKey(argNormalized)) {
      throw new SemanticCheckException(
              String.format("Parameter %s is invalid for %s function.",
                      argNormalized, getQueryName()));
    }
  }
  /**
   * Override build function because RelevanceQuery requires 2 fields,
   * but NoFieldQuery must have no fields.
   *
   * @param func : Contains function name and passed in arguments.
   * @return : QueryBuilder object
   */

  @Override
  public QueryBuilder build(FunctionExpression func) {
    var arguments = func.getArguments().stream().map(
        a -> (NamedArgumentExpression) a).collect(Collectors.toList());
    if (arguments.size() < 1) {
      throw new SyntaxCheckException(String.format(
          "%s requires at least one parameter", func.getFunctionName()));
    }

    return loadArguments(arguments);
  }


  @Override
  public T createQueryBuilder(List<NamedArgumentExpression> arguments) {
    // Extract 'query'
    var query = arguments.stream().filter(a -> a.getArgName().equalsIgnoreCase("query")).findFirst()
        .orElseThrow(() -> new SemanticCheckException("'query' parameter is missing"));

    return createBuilder(query.getValue().valueOf().stringValue());
  }

  protected abstract T createBuilder(String query);
}
