/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/** Base class for query abstraction that builds a relevance query from function expression. */
@RequiredArgsConstructor
public abstract class RelevanceQuery<T extends QueryBuilder> extends LuceneQuery {
  @Getter private final Map<String, QueryBuilderStep<T>> queryBuildActions;

  protected void ignoreArguments(List<NamedArgumentExpression> arguments) {
    arguments.removeIf(
        a ->
            a.getArgName().equalsIgnoreCase("field")
                || a.getArgName().equalsIgnoreCase("fields")
                || a.getArgName().equalsIgnoreCase("query"));
  }

  protected void checkValidArguments(String argNormalized, T queryBuilder) {
    if (!queryBuildActions.containsKey(argNormalized)) {
      throw new SemanticCheckException(
          String.format(
              "Parameter %s is invalid for %s function.",
              argNormalized, queryBuilder.getWriteableName()));
    }
  }

  protected T loadArguments(List<NamedArgumentExpression> arguments) throws SemanticCheckException {
    // Aggregate parameters by name, so getting a Map<Name:String, List>
    arguments.stream()
        .collect(Collectors.groupingBy(a -> a.getArgName().toLowerCase()))
        .forEach(
            (k, v) -> {
              if (v.size() > 1) {
                throw new SemanticCheckException(
                    String.format("Parameter '%s' can only be specified once.", k));
              }
            });

    T queryBuilder = createQueryBuilder(arguments);

    ignoreArguments(arguments);

    var iterator = arguments.listIterator();
    while (iterator.hasNext()) {
      NamedArgumentExpression arg = iterator.next();
      String argNormalized = arg.getArgName().toLowerCase();

      checkValidArguments(argNormalized, queryBuilder);

      (Objects.requireNonNull(queryBuildActions.get(argNormalized)))
          .apply(queryBuilder, arg.getValue().valueOf());
    }

    return queryBuilder;
  }

  @Override
  public QueryBuilder build(FunctionExpression func) {
    var arguments =
        func.getArguments().stream()
            .map(a -> (NamedArgumentExpression) a)
            .collect(Collectors.toList());
    if (arguments.size() < getMinimumParameterCount()) {
      throw new SyntaxCheckException(
          String.format(
              "%s requires at least %d parameter(s)", getQueryName(), getMinimumParameterCount()));
    }

    return loadArguments(arguments);
  }

  /**
   * Returns the minimum number of parameters required by the query. Subclasses can override this
   * method to allow different minimum parameter counts.
   *
   * @return minimum parameter count
   */
  protected int getMinimumParameterCount() {
    return 2; // Default: requires both fields and query
  }

  /**
   * Enrich initially created opensearch index query builder with optional arguments that are
   * wrapped in Calcite MAP RexCall.
   *
   * @param queryBuilder queryBuilder Initially created opensearch index relevance query builder
   * @param optionalArguments Map contains optional relevance query argument key value pairs
   * @return enriched QueryBuilder
   */
  protected T applyArguments(T queryBuilder, Map<String, String> optionalArguments) {
    if (optionalArguments != null && !optionalArguments.isEmpty()) {
      optionalArguments.forEach(
          (k, v) -> {
            checkValidArguments(k, queryBuilder);
            (Objects.requireNonNull(getQueryBuildActions().get(k)))
                .apply(queryBuilder, new ExprStringValue(v));
          });
    }

    return queryBuilder;
  }

  protected abstract T createQueryBuilder(List<NamedArgumentExpression> arguments);

  protected abstract String getQueryName();

  /**
   * Convenience interface for a function that updates a QueryBuilder based on ExprValue.
   *
   * @param <T> Concrete query builder
   */
  protected interface QueryBuilderStep<T extends QueryBuilder>
      extends BiFunction<T, ExprValue, T> {}
}
