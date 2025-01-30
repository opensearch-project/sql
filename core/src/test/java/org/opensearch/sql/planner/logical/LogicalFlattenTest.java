/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class LogicalFlattenTest extends AnalyzerTestBase {

  private static final String TABLE_NAME = "schema";

  @Override
  protected Map<String, ExprType> typeMapping() {
    Map<String, ExprType> mapping = new HashMap<>(super.typeMapping());

    // Override mapping for testing.
    mapping.put("struct_empty", STRUCT);

    mapping.put("struct", STRUCT);
    mapping.put("struct.integer", INTEGER);
    mapping.put("struct.double", DOUBLE);
    mapping.put("struct.nested_struct", STRUCT);
    mapping.put("struct.nested_struct.string", STRING);

    mapping.put("duplicate", STRUCT);
    mapping.put("duplicate.integer_value", INTEGER);

    return mapping;
  }

  @Test
  void testStructEmpty() {
    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("struct_empty", STRUCT));
    LogicalPlan actualLogicalPlan =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field("struct_empty")));

    assertEquals(expectedLogicalPlan, actualLogicalPlan);

    assertTypeNotDefined("struct_empty");
  }

  @Test
  void testStruct() {
    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("struct", STRUCT));
    LogicalPlan actualLogicalPlan =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field("struct")));

    assertEquals(expectedLogicalPlan, actualLogicalPlan);

    assertTypeNotDefined("struct");
    assertTypeNotDefined("struct.integer");
    assertTypeNotDefined("struct.double");
    assertTypeNotDefined("struct.nested_struct");
    assertTypeNotDefined("struct.nested_struct.string");

    assertTypeDefined("integer", INTEGER);
    assertTypeDefined("double", DOUBLE);
    assertTypeDefined("nested_struct", STRUCT);
    assertTypeDefined("nested_struct.string", STRING);
  }

  @Test
  void testStructNested() {
    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("struct.nested_struct", STRUCT));
    LogicalPlan actualLogicalPlan =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field("struct.nested_struct")));

    assertEquals(expectedLogicalPlan, actualLogicalPlan);

    assertTypeNotDefined("struct.nested_struct");
    assertTypeNotDefined("struct.nested_struct.string");

    assertTypeDefined("struct", STRUCT);
    assertTypeDefined("struct.integer", INTEGER);
    assertTypeDefined("struct.double", DOUBLE);
    assertTypeDefined("struct.string", STRING);
  }

  @Test
  void testInvalidName() {
    Flatten actualUnresolvedPlan =
        AstDSL.flatten(AstDSL.relation("schema"), AstDSL.field("invalid"));

    String msg =
        assertThrows(IllegalArgumentException.class, () -> analyze(actualUnresolvedPlan))
            .getMessage();
    assertEquals("Invalid field name for flatten command", msg);
  }

  @Test
  void testInvalidType() {
    Flatten actualUnresolvedPlan =
        AstDSL.flatten(AstDSL.relation("schema"), AstDSL.field("integer_value"));

    String actualMsg =
        assertThrows(IllegalArgumentException.class, () -> analyze(actualUnresolvedPlan))
            .getMessage();
    assertEquals(
        "Invalid field type for flatten command. Expected 'STRUCT' but got 'INTEGER'.", actualMsg);
  }

  @Test
  void testInvalidDuplicate() {
    Flatten actualUnresolvedPlan =
        AstDSL.flatten(AstDSL.relation("schema"), AstDSL.field("duplicate"));

    String msg =
        assertThrows(IllegalArgumentException.class, () -> analyze(actualUnresolvedPlan))
            .getMessage();
    assertEquals("Flatten command cannot overwrite field 'integer_value'", msg);
  }

  /** Asserts that the given field name is not defined in the type environment */
  private void assertTypeNotDefined(String fieldName) {
    Map<String, ExprType> fieldsMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertFalse(fieldsMap.containsKey(fieldName));
  }

  /**
   * Asserts that the given field name is defined in the type environment and corresponds to the
   * given type.
   */
  private void assertTypeDefined(String fieldName, ExprType fieldType) {
    Map<String, ExprType> fieldsMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertTrue(fieldsMap.containsKey(fieldName));
    assertEquals(fieldType, fieldsMap.get(fieldName));
  }
}
