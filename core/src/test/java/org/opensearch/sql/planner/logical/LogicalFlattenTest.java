/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    mapping.put("struct_basic", STRUCT);
    mapping.put("struct_basic.integer", INTEGER);
    mapping.put("struct_basic.double", DOUBLE);

    mapping.put("struct_nested", STRUCT);
    mapping.put("struct_nested.struct", STRUCT);
    mapping.put("struct_nested.struct.string", STRING);

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

    Map<String, ExprType> fieldMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertFalse(fieldMap.containsKey("struct_empty"));
  }

  @Test
  void testStructBasic() {
    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("struct_basic", STRUCT));
    LogicalPlan actualLogicalPlan =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field("struct_basic")));

    assertEquals(expectedLogicalPlan, actualLogicalPlan);

    Map<String, ExprType> fieldMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertFalse(fieldMap.containsKey("struct_basic"));
    assertFalse(fieldMap.containsKey("struct_basic.integer"));
    assertFalse(fieldMap.containsKey("struct_basic.double"));
    assertEquals(INTEGER, fieldMap.get("integer"));
    assertEquals(DOUBLE, fieldMap.get("double"));
  }

  @Test
  void testStructNested() {
    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("struct_nested", STRUCT));
    LogicalPlan actualLogicalPlan =
        analyze(AstDSL.flatten(AstDSL.relation(TABLE_NAME), AstDSL.field("struct_nested")));

    assertEquals(expectedLogicalPlan, actualLogicalPlan);

    Map<String, ExprType> fieldMap =
        analysisContext.peek().lookupAllTupleFields(Namespace.FIELD_NAME);
    assertFalse(fieldMap.containsKey("struct_nested"));
    assertFalse(fieldMap.containsKey("struct_nested.struct"));
    assertEquals(STRING, fieldMap.get("string"));
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
    assertEquals("Invalid field type 'INTEGER' for flatten command", actualMsg);
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
}
