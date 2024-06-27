package org.opensearch.sql.sql.parser;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Having;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

/**
 * AST Having Aggregation builder that creates a {@link Having} clause with condition expressions
 * and all aggregators in Having. Those aggregators will be pushed down to the underlying {@link
 * LogicalAggregation}.
 */
@RequiredArgsConstructor
public class AstHavingAggregationBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {
  private final QuerySpecification querySpec;
  private final UnresolvedExpression condition;

  @Override
  public UnresolvedPlan visitHavingClause(OpenSearchSQLParser.HavingClauseContext ctx) {
    return new Having(createAggregators(), condition);
  }

  private List<UnresolvedExpression> createAggregators() {
    return new ArrayList<>(querySpec.getAggregatorsInHaving());
  }
}
