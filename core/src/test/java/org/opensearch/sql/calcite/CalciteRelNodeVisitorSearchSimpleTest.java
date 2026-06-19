/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;

/**
 * Simple tests for CalciteRelNodeVisitor.visitSearch method. Tests basic functionality without
 * complex mocking.
 */
public class CalciteRelNodeVisitorSearchSimpleTest {

  private CalciteRelNodeVisitor visitor;
  @Mock DataSourceService dataSourceService;

  @BeforeEach
  public void setUp() {
    visitor = new CalciteRelNodeVisitor(dataSourceService);
  }

  @Test
  public void testVisitSearchRequiresContext() {
    // Arrange
    String queryString = "field:value";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));
    Search searchNode = new Search(relation, queryString);

    // Act & Assert - should throw NPE without proper context
    assertThrows(
        NullPointerException.class,
        () -> {
          visitor.visitSearch(searchNode, null);
        });
  }

  @Test
  public void testSearchNodeStructure() {
    // Arrange
    String queryString = "field1:value1 AND field2:value2";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));

    // Act
    Search searchNode = new Search(relation, queryString);

    // Assert
    assertNotNull(searchNode);
    assertEquals(queryString, searchNode.getQueryString());
    assertNotNull(searchNode.getChild());
    assertEquals(1, searchNode.getChild().size());
    assertEquals(relation, searchNode.getChild().get(0));
  }

  @Test
  public void testSearchWithEmptyQuery() {
    // Arrange
    String queryString = "";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));

    // Act
    Search searchNode = new Search(relation, queryString);

    // Assert
    assertEquals("", searchNode.getQueryString());
    assertEquals(relation, searchNode.getChild().get(0));
  }

  @Test
  public void testSearchWithComplexQuery() {
    // Arrange
    String queryString = "(field1:value1 OR field2:value2) AND NOT field3:value3";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));

    // Act
    Search searchNode = new Search(relation, queryString);

    // Assert
    assertEquals(queryString, searchNode.getQueryString());
  }

  @Test
  public void testSearchWithSpecialCharacters() {
    // Arrange
    String queryString = "field:\"value with spaces\" AND field2:wildcard*";
    Relation relation = new Relation(AstDSL.qualifiedName("test_index"));

    // Act
    Search searchNode = new Search(relation, queryString);

    // Assert
    assertEquals(queryString, searchNode.getQueryString());
  }

  /**
   * Table functions (e.g. SQL {@code vectorSearch(...)}) are unsupported on the Calcite /
   * analytics-engine path. The visitor must surface this as a structured {@link ErrorReport} coded
   * {@link ErrorCode#UNSUPPORTED_OPERATION} (so the REST layer returns a 4xx, not a 500), while
   * preserving the {@link CalciteUnsupportedException} cause so the v2-fallback detection in
   * QueryService still recognizes it.
   */
  @Test
  public void testVisitTableFunctionThrowsUnsupportedOperationErrorReport() {
    TableFunction tableFunction =
        new TableFunction(AstDSL.qualifiedName("vectorSearch"), List.of());

    ErrorReport report =
        assertThrows(ErrorReport.class, () -> visitor.visitTableFunction(tableFunction, null));

    assertEquals(ErrorCode.UNSUPPORTED_OPERATION, report.getCode());
    assertInstanceOf(CalciteUnsupportedException.class, report.getCause());
    assertNotNull(report.getSuggestion());
  }
}
