/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

/**
 *
 *
 * <pre>SelectExpressionAnalyzerTest
 * AST aggregation builder that builds AST aggregation node for the following scenarios:
 *
 *  1. Explicit GROUP BY
 *     1.1 Group by column name or scalar expression (SELECT DISTINCT equivalent):
 *          SELECT ABS(age) FROM test GROUP BY ABS(age)
 *     1.2 Group by alias in SELECT AS clause:
 *          SELECT state AS s FROM test GROUP BY s
 *     1.3 Group by ordinal referring to select list:
 *          SELECT state FROM test GROUP BY 1
 *  2. Implicit GROUP BY
 *     2.1 No non-aggregated item (only aggregate functions):
 *          SELECT AVG(age), SUM(balance) FROM test
 *     2.2 Non-aggregated item exists:
 *          SELECT state, AVG(age) FROM test
 *
 *  For 1.1 and 2.1, Aggregation node is built with aggregators.
 *  For 1.2 and 1.3, alias and ordinal is replaced first and then
 *    Aggregation is built same as above.
 *  For 2.2, Exception thrown for now. We may support this by different SQL mode.
 *
 * Note the responsibility separation between this builder and analyzer in core engine:
 *
 *  1. This builder is only responsible for AST node building and handle special SQL
 *     syntactical cases aforementioned. The validation in this builder is essentially
 *     static based on syntactic information.
 *  2. Analyzer will perform semantic check and report semantic error as needed.
 * </pre>
 */
@RequiredArgsConstructor
public class AstAggregationBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {

  /** Query specification that contains info collected beforehand. */
  private final QuerySpecification querySpec;

  @Override
  public UnresolvedPlan visit(ParseTree groupByClause) {
    if (querySpec.getGroupByItems().isEmpty()) {
      if (isAggregatorNotFoundAnywhere()) {
        // Simple select query without GROUP BY and aggregate function in SELECT
        return null;
      }
      return buildImplicitAggregation();
    }
    return buildExplicitAggregation();
  }

  private UnresolvedPlan buildExplicitAggregation() {
    List<UnresolvedExpression> groupByItems = replaceGroupByItemIfAliasOrOrdinal();
    return new Aggregation(new ArrayList<>(querySpec.getAggregators()), List.of(), groupByItems);
  }

  private UnresolvedPlan buildImplicitAggregation() {
    Optional<UnresolvedExpression> invalidSelectItem = findNonAggregatedItemInSelect();

    if (invalidSelectItem.isPresent()) {
      // Report semantic error to avoid fall back to old engine again
      throw new SemanticCheckException(
          StringUtils.format(
              "Explicit GROUP BY clause is required because expression [%s] "
                  + "contains non-aggregated column",
              invalidSelectItem.get()));
    }

    return new Aggregation(
        new ArrayList<>(querySpec.getAggregators()), List.of(), querySpec.getGroupByItems());
  }

  private List<UnresolvedExpression> replaceGroupByItemIfAliasOrOrdinal() {
    return querySpec.getGroupByItems().stream()
        .map(querySpec::replaceIfAliasOrOrdinal)
        .map(expr -> new Alias(expr.toString(), expr))
        .collect(Collectors.toList());
  }

  /**
   * Find non-aggregate item in SELECT clause. Note that literal is special which is not required to
   * be applied by aggregate function.
   */
  private Optional<UnresolvedExpression> findNonAggregatedItemInSelect() {
    return querySpec.getSelectItems().stream()
        .filter(this::isNonAggregateOrLiteralExpression)
        .findFirst();
  }

  private boolean isAggregatorNotFoundAnywhere() {
    return querySpec.getAggregators().isEmpty();
  }

  private boolean isNonAggregateOrLiteralExpression(UnresolvedExpression expr) {
    if (expr instanceof AggregateFunction) {
      return false;
    }

    if (expr instanceof QualifiedName) {
      return true;
    }

    List<? extends Node> children = expr.getChild();
    return children.stream()
        .anyMatch(child -> isNonAggregateOrLiteralExpression((UnresolvedExpression) child));
  }
}
