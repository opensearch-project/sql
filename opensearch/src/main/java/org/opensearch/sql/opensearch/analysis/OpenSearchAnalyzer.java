/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.analysis;

import java.util.List;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.NamedExpressionAnalyzer;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.logical.LogicalPlan;

public class OpenSearchAnalyzer extends Analyzer {

  public OpenSearchAnalyzer(DataSourceService dataSourceService,
                            BuiltinFunctionRepository repository) {
    this(dataSourceService, repository, new OpenSearchExpressionAnalyzer(repository));
  }

  private OpenSearchAnalyzer(DataSourceService dataSourceService,
                             BuiltinFunctionRepository repository,
                             OpenSearchExpressionAnalyzer expressionAnalyzer) {
    super(
        expressionAnalyzer,
        new OpenSearchSelectExpressionAnalyzer(expressionAnalyzer),
        new NamedExpressionAnalyzer(expressionAnalyzer),
        dataSourceService,
        repository);
  }

  @Override
  public LogicalPlan visitProjectList(List<UnresolvedExpression> columns, List<NamedExpression> namedExpressions, LogicalPlan child, AnalysisContext context) {
    for (UnresolvedExpression expr : columns) {
      HighlightAnalyzer highlightAnalyzer = new HighlightAnalyzer(expressionAnalyzer, child);
      child = highlightAnalyzer.analyze(expr, context);

      NestedAnalyzer nestedAnalyzer =
          new NestedAnalyzer(namedExpressions, expressionAnalyzer, child);
      child = nestedAnalyzer.analyze(expr, context);
    }
    return super.visitProjectList(columns, namedExpressions, child, context);
  }

  /**
   * Ensure NESTED function is not used in GROUP BY, and HAVING clauses. Fallback to legacy engine.
   * Can remove when support is added for NESTED function in WHERE, GROUP BY, ORDER BY, and HAVING
   * clauses.
   *
   * @param condition : Filter condition
   */
  public void verifySupportsCondition(Expression condition) {
    if (condition instanceof FunctionExpression) {
      if (((FunctionExpression) condition)
          .getFunctionName()
          .getFunctionName()
          .equalsIgnoreCase(BuiltinFunctionName.NESTED.name())) {
        throw new SyntaxCheckException(
            "Falling back to legacy engine. Nested function is not supported in WHERE,"
                + " GROUP BY, and HAVING clauses.");
      }
      ((FunctionExpression) condition)
          .getArguments().forEach(this::verifySupportsCondition);
    }
  }
}