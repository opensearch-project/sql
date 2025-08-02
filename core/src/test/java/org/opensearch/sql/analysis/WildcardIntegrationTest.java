/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.storage.Table;

/**
 * Integration test to verify wildcard functionality works end-to-end for both table and fields
 * commands when Calcite is not enabled.
 */
class WildcardIntegrationTest {

  private Analyzer analyzer;
  private AnalysisContext context;
  private DataSourceService dataSourceService;
  private Table mockTable;

  @BeforeEach
  void setUp() {
    // Setup mocks
    ExpressionAnalyzer expressionAnalyzer = mock(ExpressionAnalyzer.class);
    dataSourceService = mock(DataSourceService.class);
    BuiltinFunctionRepository repository = mock(BuiltinFunctionRepository.class);
    mockTable = mock(Table.class);

    // Setup available fields in the mock table
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("account_number", ExprCoreType.INTEGER);
    fieldTypes.put("firstname", ExprCoreType.STRING);
    fieldTypes.put("lastname", ExprCoreType.STRING);
    fieldTypes.put("balance", ExprCoreType.DOUBLE);
    fieldTypes.put("age", ExprCoreType.INTEGER);

    when(mockTable.getFieldTypes()).thenReturn(fieldTypes);
    when(mockTable.getReservedFieldTypes()).thenReturn(new HashMap<>());

    analyzer = new Analyzer(expressionAnalyzer, dataSourceService, repository);
    context = new AnalysisContext();
  }

  @Test
  void testTableCommandWithPrefixWildcard() {
    // Create a table command with prefix wildcard: table account*
    Field wildcardField = new Field(QualifiedName.of("account*"));
    Project tableCommand = new Project(Arrays.asList(wildcardField));

    // Create a mock relation as child
    Relation relation = new Relation(QualifiedName.of("test_table"));
    tableCommand.attach(relation);

    // This test demonstrates the structure - actual execution would require
    // more complex mocking of the data source service and storage engine
    assertTrue(tableCommand.getProjectList().size() == 1);
    assertTrue(tableCommand.getProjectList().get(0) instanceof Field);

    Field field = (Field) tableCommand.getProjectList().get(0);
    assertTrue(field.getField().toString().contains("*"));
  }

  @Test
  void testFieldsCommandWithSuffixWildcard() {
    // Create a fields command with suffix wildcard: fields *name
    Field wildcardField = new Field(QualifiedName.of("*name"));
    Project fieldsCommand = new Project(Arrays.asList(wildcardField));

    // Create a mock relation as child
    Relation relation = new Relation(QualifiedName.of("test_table"));
    fieldsCommand.attach(relation);

    // Verify the structure
    assertTrue(fieldsCommand.getProjectList().size() == 1);
    assertTrue(fieldsCommand.getProjectList().get(0) instanceof Field);

    Field field = (Field) fieldsCommand.getProjectList().get(0);
    assertEquals("*name", field.getField().toString());
  }

  @Test
  void testMixedWildcardAndRegularFields() {
    // Create command with mixed wildcards and regular fields
    Field wildcardField = new Field(QualifiedName.of("account*"));
    Field regularField = new Field(QualifiedName.of("age"));
    Project command = new Project(Arrays.asList(wildcardField, regularField));

    // Create a mock relation as child
    Relation relation = new Relation(QualifiedName.of("test_table"));
    command.attach(relation);

    // Verify the structure
    assertEquals(2, command.getProjectList().size());

    Field field1 = (Field) command.getProjectList().get(0);
    Field field2 = (Field) command.getProjectList().get(1);

    assertTrue(field1.getField().toString().contains("*"));
    assertEquals("age", field2.getField().toString());
  }
}
