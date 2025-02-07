/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
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
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class LogicalFlattenTest extends AnalyzerTestBase {

  // Test constants
  private static final String TABLE_NAME = "schema";

  @Override
  protected Map<String, ExprType> typeMapping() {
    Map<String, ExprType> mapping = new HashMap<>(super.typeMapping());

    // Override mapping for testing.
    mapping.put("struct_empty", STRUCT);

    mapping.put("struct", STRUCT);
    mapping.put("struct.integer", INTEGER);
    mapping.put("struct.double", DOUBLE);
    mapping.put("struct.struct_nested", STRUCT);
    mapping.put("struct.struct_nested.string", STRING);
    mapping.put("struct.struct_nested.struct_nested_deep", STRUCT);
    mapping.put("struct.struct_nested.struct_nested_deep.boolean", BOOLEAN);

    mapping.put("duplicate", STRUCT);
    mapping.put("duplicate.integer_value", INTEGER);

    mapping.put("duplicate_2", STRUCT);
    mapping.put("duplicate_2.integer_value", INTEGER);
    mapping.put("duplicate_2.double_value", INTEGER);

    return mapping;
  }

  @Test
  void testStructEmpty() {
    executeFlatten("struct_empty");

    assertTypeDefined("struct_empty", STRUCT);
  }

  @Test
  void testStruct() {
    executeFlatten("struct");

    assertTypeDefined("struct", STRUCT);
    assertTypeDefined("struct.integer", INTEGER);
    assertTypeDefined("struct.double", DOUBLE);
    assertTypeDefined("struct.struct_nested", STRUCT);
    assertTypeDefined("struct.struct_nested.string", STRING);

    assertTypeDefined("struct", STRUCT);
    assertTypeDefined("integer", INTEGER);
    assertTypeDefined("double", DOUBLE);
    assertTypeDefined("struct_nested", STRUCT);
    assertTypeDefined("struct_nested.string", STRING);
  }

  @Test
  void testStructNested() {
    executeFlatten("struct.struct_nested");

    assertTypeDefined("struct", STRUCT);
    assertTypeDefined("struct.struct_nested", STRUCT);
    assertTypeDefined("struct.struct_nested.string", STRING);

    assertTypeDefined("struct.string", STRING);
  }

  @Test
  void testStructNestedDeep() {
    executeFlatten("struct.struct_nested.struct_nested_deep");

    assertTypeDefined("struct", STRUCT);
    assertTypeDefined("struct.struct_nested", STRUCT);
    assertTypeDefined("struct.struct_nested.struct_nested_deep", STRUCT);
    assertTypeDefined("struct.struct_nested.struct_nested_deep.boolean", BOOLEAN);

    assertTypeDefined("struct.struct_nested.boolean", BOOLEAN);
  }

  @Test
  void testInvalidName() {
    Exception ex;

    ex = assertThrows(SemanticCheckException.class, () -> executeFlatten("invalid"));
    assertEquals(
        "can't resolve Symbol(namespace=FIELD_NAME, name=invalid) in type env", ex.getMessage());

    ex = assertThrows(SemanticCheckException.class, () -> executeFlatten(".invalid"));
    assertEquals(
        "can't resolve Symbol(namespace=FIELD_NAME, name=.invalid) in type env", ex.getMessage());

    ex = assertThrows(SemanticCheckException.class, () -> executeFlatten("invalid."));
    assertEquals(
        "can't resolve Symbol(namespace=FIELD_NAME, name=invalid.) in type env", ex.getMessage());
  }

  @Test
  void testInvalidDuplicate() {
    Exception ex;

    ex = assertThrows(SemanticCheckException.class, () -> executeFlatten("duplicate"));
    assertEquals("Flatten command cannot overwrite fields: integer_value", ex.getMessage());

    ex = assertThrows(SemanticCheckException.class, () -> executeFlatten("duplicate_2"));
    assertEquals(
        "Flatten command cannot overwrite fields: integer_value, double_value", ex.getMessage());
  }

  /**
   * Builds the actual and expected logical plans by flattening the field with the given name, and
   * tests whether they are equal.
   */
  private void executeFlatten(String fieldName) {
    LogicalPlan expected =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref(fieldName, STRUCT));
    LogicalPlan actual =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field(fieldName)));
    assertEquals(expected, actual);
  }

  /** Asserts that the given field name is defined in the type environment with the given type. */
  private void assertTypeDefined(String fieldName, ExprType fieldType) {
    Map<String, ExprType> fieldsMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertTrue(fieldsMap.containsKey(fieldName));
    assertEquals(fieldType, fieldsMap.get(fieldName));
  }
}
