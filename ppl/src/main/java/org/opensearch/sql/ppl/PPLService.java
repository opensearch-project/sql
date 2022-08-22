/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.NativeQuery;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.utils.LogUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizer;
import org.opensearch.sql.ppl.utils.UnresolvedPlanHelper;
import org.opensearch.sql.storage.StorageEngine;

@RequiredArgsConstructor
public class PPLService {
  private final PPLSyntaxParser parser;

  private final StorageEngine openSearchStorageEngine;

  private final ExecutionEngine openSearchExecutionEngine;

  private final BuiltinFunctionRepository repository;

  private final CatalogService catalogService;

  private final PPLQueryDataAnonymizer anonymizer = new PPLQueryDataAnonymizer();

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Execute the {@link PPLQueryRequest}, using {@link ResponseListener} to get response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener}
   */
  public void execute(PPLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      openSearchExecutionEngine.execute(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link PPLQueryRequest} using {@link ResponseListener} to
   * get and format explain response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(PPLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      openSearchExecutionEngine.explain(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private PhysicalPlan plan(PPLQueryRequest request) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getRequest());
    UnresolvedPlan ast = cst.accept(
        new AstBuilder(new AstExpressionBuilder(), catalogService, request.getRequest()));
    LOG.info("[{}] Incoming request {}", LogUtils.getRequestId(), anonymizer.anonymizeData(ast));

    StorageEngine storageEngine =
        catalogService.getStorageEngine(getConnector(ast)).orElse(openSearchStorageEngine);

    // 2.Analyze abstract syntax to generate logical plan
    LogicalPlan logicalPlan =
        new Analyzer(new ExpressionAnalyzer(repository), storageEngine).analyze(
            UnresolvedPlanHelper.addSelectAll(ast),
            new AnalysisContext());

    // 3.Generate optimal physical plan from logical plan
    return new Planner(storageEngine, LogicalPlanOptimizer.create(new DSL(repository)))
        .plan(logicalPlan);
  }

  private String getConnector(UnresolvedPlan unresolvedPlan) {
    String connector =  unresolvedPlan.accept(new AbstractNodeVisitor<>() {
      @Override
      public String visitNativeQuery(NativeQuery node, AnalysisContext context) {
        return node.getCatalogName();
      }

      @Override
      public String visitRelation(Relation node, AnalysisContext context) {
        return node.getCatalogName();
      }
    }, new AnalysisContext());
    return connector == null ? "opensearch" : connector;
  }

}
