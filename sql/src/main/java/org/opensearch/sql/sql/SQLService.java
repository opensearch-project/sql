/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstDDLBuilder;
import org.opensearch.sql.storage.StorageEngine;

/**
 * SQL service.
 */
@RequiredArgsConstructor
public class SQLService {

  private final SQLSyntaxParser parser;

  private final Analyzer analyzer;

  private final StorageEngine storageEngine;

  private final ExecutionEngine executionEngine;

  private final BuiltinFunctionRepository repository;

  private final QueryService queryService;

  /**
   * Parse, analyze and plan the SQL query.
   */
  public PhysicalPlan plan(SQLQueryRequest request) {
    return queryService.plan(parse(request.getQuery()));
  }

  /**
   * Parse, analyze, plan and execute the query.
   * @param request       SQL query request
   * @param listener      callback listener
   */
  public void execute(SQLQueryRequest request, ResponseListener<QueryResponse> listener) {
    queryService.execute(parse(request.getQuery()), listener);
  }

  /**
   * Given physical plan, execute it and listen on response.
   * @param plan        physical plan
   * @param listener    callback listener
   */
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    try {
      executionEngine.execute(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Given physical plan, explain it.
   * @param plan        physical plan
   * @param listener    callback listener
   */
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    try {
      executionEngine.explain(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Parse query and convert parse tree (CST) to abstract syntax tree (AST).
   */
  public UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    AstBuilder astBuilder = new AstBuilder(query);
    AstDDLBuilder astDdlBuilder = new AstDDLBuilder(astBuilder);

    DataDefinitionPlan ddl = astDdlBuilder.build(cst);
    if (ddl.getTask() != null) {
      return ddl;
    }
    return cst.accept(astBuilder);
  }
}
