/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
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

  private final Analyzer analyzer;

  private final StorageEngine storageEngine;

  private final ExecutionEngine executionEngine;

  private final BuiltinFunctionRepository repository;

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
      executionEngine.execute(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link PPLQueryRequest} using {@link ResponseListener} to
   * get and format explain response.
   *
   * @param request {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(PPLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      executionEngine.explain(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private PhysicalPlan plan(PPLQueryRequest request) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.analyzeSyntax(request.getRequest());
    UnresolvedPlan ast = cst.accept(
        new AstBuilder(new AstExpressionBuilder(), request.getRequest()));

    LOG.info("[{}] Incoming request {}", LogUtils.getRequestId(), anonymizer.anonymizeData(ast));

    // 2.Analyze abstract syntax to generate logical plan
    LogicalPlan logicalPlan = analyzer.analyze(UnresolvedPlanHelper.addSelectAll(ast),
        new AnalysisContext());

    // 3.Generate optimal physical plan from logical plan
    return new Planner(storageEngine, LogicalPlanOptimizer.create(new DSL(repository)))
        .plan(logicalPlan);
  }

}
