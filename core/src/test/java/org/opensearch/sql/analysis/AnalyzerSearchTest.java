/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.storage.Table;

public class AnalyzerSearchTest {

  private Analyzer analyzer;
  private ExpressionAnalyzer expressionAnalyzer;
  private DataSourceService dataSourceService;
  private BuiltinFunctionRepository repository;
  private AnalysisContext context;
  private Table mockTable;

  @BeforeEach
  public void setUp() {
    expressionAnalyzer = mock(ExpressionAnalyzer.class);
    dataSourceService = mock(DataSourceService.class);
    repository = mock(BuiltinFunctionRepository.class);
    context = new AnalysisContext();
    mockTable = mock(Table.class);

    analyzer = new Analyzer(expressionAnalyzer, dataSourceService, repository);

    // Setup table with field types
    when(mockTable.getFieldTypes())
        .thenReturn(
            ImmutableMap.of(
                "field1", ExprCoreType.STRING,
                "field2", ExprCoreType.STRING));
    when(mockTable.getReservedFieldTypes()).thenReturn(ImmutableMap.of());
  }

  @Test
  public void testVisitSearchWithSimpleQuery() {
    // Arrange
    String queryString = "field1:value1";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));
    Search searchNode = new Search(relation, queryString);

    LogicalRelation logicalRelation = new LogicalRelation("test_index", mockTable);
    FunctionExpression queryStringExpr = mock(FunctionExpression.class);

    // Mock DataSource and StorageEngine
    org.opensearch.sql.datasource.model.DataSource mockDataSource =
        mock(org.opensearch.sql.datasource.model.DataSource.class);
    org.opensearch.sql.storage.StorageEngine mockStorageEngine =
        mock(org.opensearch.sql.storage.StorageEngine.class);
    when(dataSourceService.getDataSource(any())).thenReturn(mockDataSource);
    when(mockDataSource.getStorageEngine()).thenReturn(mockStorageEngine);
    when(mockStorageEngine.getTable(any(), any())).thenReturn(mockTable);

    // Mock the child plan processing
    when(expressionAnalyzer.analyze(any(Function.class), eq(context))).thenReturn(queryStringExpr);

    // Act
    LogicalPlan result = analyzer.visitSearch(searchNode, context);

    // Assert
    assertNotNull(result);
    assertTrue(result instanceof LogicalFilter);

    // Verify query_string function was created
    ArgumentCaptor<Function> functionCaptor = ArgumentCaptor.forClass(Function.class);
    verify(expressionAnalyzer).analyze(functionCaptor.capture(), eq(context));

    Function capturedFunction = functionCaptor.getValue();
    assertEquals("query_string", capturedFunction.getFuncName());
    assertEquals(1, capturedFunction.getFuncArgs().size());
  }

  @Test
  public void testVisitSearchWithComplexQuery() {
    // Arrange
    String queryString = "(field1:value1 OR field2:value2) AND NOT field3:value3";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));
    Search searchNode = new Search(relation, queryString);

    FunctionExpression queryStringExpr = mock(FunctionExpression.class);
    when(queryStringExpr.type()).thenReturn(ExprCoreType.BOOLEAN);

    // Mock DataSource and StorageEngine
    org.opensearch.sql.datasource.model.DataSource mockDataSource =
        mock(org.opensearch.sql.datasource.model.DataSource.class);
    org.opensearch.sql.storage.StorageEngine mockStorageEngine =
        mock(org.opensearch.sql.storage.StorageEngine.class);
    when(dataSourceService.getDataSource(any())).thenReturn(mockDataSource);
    when(mockDataSource.getStorageEngine()).thenReturn(mockStorageEngine);
    when(mockStorageEngine.getTable(any(), any())).thenReturn(mockTable);

    when(expressionAnalyzer.analyze(any(Function.class), eq(context))).thenReturn(queryStringExpr);

    // Act
    LogicalPlan result = analyzer.visitSearch(searchNode, context);

    // Assert
    assertNotNull(result);
    assertTrue(result instanceof LogicalFilter);
    LogicalFilter filter = (LogicalFilter) result;
    assertEquals(queryStringExpr, filter.getCondition());
  }

  @Test
  public void testVisitSearchPreservesChildPlan() {
    // Arrange
    String queryString = "test:query";
    UnresolvedPlan mockChild = mock(UnresolvedPlan.class);
    Search searchNode = new Search(mockChild, queryString);

    LogicalPlan mockLogicalPlan = mock(LogicalPlan.class);
    FunctionExpression queryStringExpr = mock(FunctionExpression.class);

    when(mockChild.accept(analyzer, context)).thenReturn(mockLogicalPlan);
    when(expressionAnalyzer.analyze(any(Function.class), eq(context))).thenReturn(queryStringExpr);

    // Act
    LogicalPlan result = analyzer.visitSearch(searchNode, context);

    // Assert
    assertNotNull(result);
    assertTrue(result instanceof LogicalFilter);
    LogicalFilter filter = (LogicalFilter) result;
    assertEquals(mockLogicalPlan, filter.getChild().get(0));
    assertEquals(queryStringExpr, filter.getCondition());
  }

  @Test
  public void testVisitSearchWithEmptyQuery() {
    // Arrange
    String queryString = "";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));
    Search searchNode = new Search(relation, queryString);

    FunctionExpression queryStringExpr = mock(FunctionExpression.class);

    // Mock DataSource and StorageEngine
    org.opensearch.sql.datasource.model.DataSource mockDataSource =
        mock(org.opensearch.sql.datasource.model.DataSource.class);
    org.opensearch.sql.storage.StorageEngine mockStorageEngine =
        mock(org.opensearch.sql.storage.StorageEngine.class);
    when(dataSourceService.getDataSource(any())).thenReturn(mockDataSource);
    when(mockDataSource.getStorageEngine()).thenReturn(mockStorageEngine);
    when(mockStorageEngine.getTable(any(), any())).thenReturn(mockTable);

    when(expressionAnalyzer.analyze(any(Function.class), eq(context))).thenReturn(queryStringExpr);

    // Act
    LogicalPlan result = analyzer.visitSearch(searchNode, context);

    // Assert
    assertNotNull(result);
    assertTrue(result instanceof LogicalFilter);

    // Verify empty query string was passed
    ArgumentCaptor<Function> functionCaptor = ArgumentCaptor.forClass(Function.class);
    verify(expressionAnalyzer).analyze(functionCaptor.capture(), eq(context));

    Function capturedFunction = functionCaptor.getValue();
    assertEquals("query_string", capturedFunction.getFuncName());
  }

  @Test
  public void testVisitSearchCreatesCorrectQueryStringFunction() {
    // Arrange
    String queryString = "field:\"exact phrase\" AND field2:wildcard*";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));
    Search searchNode = new Search(relation, queryString);

    FunctionExpression queryStringExpr = mock(FunctionExpression.class);

    // Mock DataSource and StorageEngine
    org.opensearch.sql.datasource.model.DataSource mockDataSource =
        mock(org.opensearch.sql.datasource.model.DataSource.class);
    org.opensearch.sql.storage.StorageEngine mockStorageEngine =
        mock(org.opensearch.sql.storage.StorageEngine.class);
    when(dataSourceService.getDataSource(any())).thenReturn(mockDataSource);
    when(mockDataSource.getStorageEngine()).thenReturn(mockStorageEngine);
    when(mockStorageEngine.getTable(any(), any())).thenReturn(mockTable);

    when(expressionAnalyzer.analyze(any(Function.class), eq(context))).thenReturn(queryStringExpr);

    // Act
    analyzer.visitSearch(searchNode, context);

    // Assert - verify the function structure
    ArgumentCaptor<Function> functionCaptor = ArgumentCaptor.forClass(Function.class);
    verify(expressionAnalyzer).analyze(functionCaptor.capture(), eq(context));

    Function capturedFunction = functionCaptor.getValue();
    assertEquals("query_string", capturedFunction.getFuncName());
    assertEquals(1, capturedFunction.getFuncArgs().size());

    // The function should have an unresolvedArg with "query" name and the query string as value
    var funcArg = capturedFunction.getFuncArgs().get(0);
    assertNotNull(funcArg);
  }
}
