/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Lucene query that build nested query.
 */
public class NestedQuery extends LuceneQuery {

  /**
   * Build query for 'nested' function used in predicate expression. Supports 'nested' function on
   * left and literal on right.
   * @param func Function expression.
   * @param innerQuery Comparison query to be place inside nested query.
   * @return Nested query.
   */
  public QueryBuilder buildNested(FunctionExpression func, LuceneQuery innerQuery) {
    // Generate inner query for placement inside nested query
    FunctionExpression nestedFunc = (FunctionExpression)func.getArguments().get(0);
    validateArgs(nestedFunc, func.getArguments().get(1));
    ExprValue literalValue = func.getArguments().get(1).valueOf();
    ReferenceExpression ref = (ReferenceExpression) nestedFunc.getArguments().get(0);
    QueryBuilder innerQueryResult =
        innerQuery.doBuild(ref.getAttr(), nestedFunc.type(), literalValue);

    // Generate nested query
    boolean hasPathParam = nestedFunc.getArguments().size() == 2;
    String pathStr = hasPathParam ? nestedFunc.getArguments().get(1).toString() :
        getNestedPathString((ReferenceExpression) nestedFunc.getArguments().get(0));
    return QueryBuilders.nestedQuery(pathStr, innerQueryResult, ScoreMode.None);
  }

  /**
   * Dynamically generate path for nested field. An example field of 'office.section.cubicle'
   * would dynamically generate the path 'office.section'.
   * @param field nested field to generate path for.
   * @return path for nested field.
   */
  private String getNestedPathString(ReferenceExpression field) {
    String ret = "";
    for (int i = 0; i < field.getPaths().size() - 1; i++) {
      ret += (i == 0) ? field.getPaths().get(i) : "." + field.getPaths().get(i);
    }
    return ret;
  }

  /**
   * Validate arguments in nested function and predicate expression.
   * @param nestedFunc Nested function expression.
   */
  private void validateArgs(FunctionExpression nestedFunc, Expression rightExpression) {
    if (nestedFunc.getArguments().size() > 2) {
      throw new IllegalArgumentException(
          "nested function supports 2 parameters (field, path) or 1 parameter (field)"
      );
    }

    for (var arg : nestedFunc.getArguments()) {
      if (!(arg instanceof ReferenceExpression)) {
        throw new IllegalArgumentException(
            String.format("Illegal nested field name: %s",
                arg.toString()
            )
        );
      }
    }

    if (!(rightExpression instanceof LiteralExpression)) {
      throw new IllegalArgumentException(
          String.format("Illegal argument on right side of predicate expression: %s",
              rightExpression.toString()
          )
      );
    }
  }
}
