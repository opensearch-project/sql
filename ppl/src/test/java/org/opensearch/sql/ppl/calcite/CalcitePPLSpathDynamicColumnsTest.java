/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.calcite.utils.DynamicColumnProcessor;

public class CalcitePPLSpathDynamicColumnsTest extends CalcitePPLAbstractTest {

  public CalcitePPLSpathDynamicColumnsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSpathDynamicColumnsGeneration() {
    String ppl = "source=EMP | spath input=ENAME";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));

    int dynamicColumnsIndex =
        rowType.getFieldNames().indexOf(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);
    RelDataType dynamicColumnsType = rowType.getFieldList().get(dynamicColumnsIndex).getType();
    assertEquals(SqlTypeName.MAP, dynamicColumnsType.getSqlTypeName());
  }

  @Test
  public void testSpathSpecificPathExtraction() {
    String ppl = "source=EMP | spath input=ENAME path=name output=extracted_name";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains("extracted_name"));
    assertFalse(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));
  }

  @Test
  public void testDynamicFieldResolution() {
    String ppl = "source=EMP | spath input=ENAME | eval test_field = unknown_field";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains("test_field"));
  }

  @Test
  public void testSpathWithJsonData() {
    String ppl =
        "source=EMP | eval json_field = '{\"name\":\"John\",\"age\":30,\"city\":\"New"
            + " York\",\"skills\":[\"Java\",\"Python\"]}' | spath input=json_field";

    RelNode root = getRelNode(ppl);
    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));

    int dynamicColumnsIndex =
        rowType.getFieldNames().indexOf(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);
    RelDataType dynamicColumnsType = rowType.getFieldList().get(dynamicColumnsIndex).getType();
    assertEquals(SqlTypeName.MAP, dynamicColumnsType.getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, dynamicColumnsType.getKeyType().getSqlTypeName());
    assertEquals(SqlTypeName.ANY, dynamicColumnsType.getValueType().getSqlTypeName());
  }

  @Test
  public void testSpathWithComplexJsonData() {
    String ppl =
        "source=EMP | eval complex_json ="
            + " '{\"user\":{\"profile\":{\"name\":\"Alice\",\"preferences\":{\"theme\":\"dark\",\"language\":\"en\"}}},\"metadata\":{\"created\":\"2023-01-01\",\"tags\":[\"important\",\"user\"]}}'"
            + " | spath input=complex_json";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));
  }

  @Test
  public void testDynamicFieldAccessAfterSpath() {
    String ppl =
        "source=EMP | eval json_data ="
            + " '{\"firstName\":\"John\",\"lastName\":\"Doe\",\"department\":\"Engineering\"}' |"
            + " spath input=json_data | eval full_name = firstName + ' ' + lastName";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains("full_name"));
  }

  @Test
  public void testSpathWithSpecificPath() {
    String specificPathPpl =
        "source=EMP | eval json_data = '{\"name\":\"John\",\"age\":30}'"
            + " | spath input=json_data path=name output=extracted_name";

    RelNode specificRoot = getRelNode(specificPathPpl);
    RelDataType rowType = specificRoot.getRowType();
    assertTrue(rowType.getFieldNames().contains("extracted_name"));
  }

  @Test
  public void testSpathWithDynamicColumns() {
    String dynamicPpl =
        "source=EMP | eval json_data = '{\"name\":\"John\",\"age\":30}' | spath input=json_data";
    RelNode dynamicRoot = getRelNode(dynamicPpl);

    RelDataType rowType = dynamicRoot.getRowType();
    assertTrue(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));
  }

  @Test
  public void testMultipleSpathOperations() {
    String ppl =
        "source=EMP | eval json1 = '{\"user\":\"Alice\",\"role\":\"admin\"}'"
            + " | eval json2 = '{\"project\":\"OpenSearch\",\"team\":\"SQL\"}'"
            + " | spath input=json1"
            + " | spath input=json2";
    RelNode root = getRelNode(ppl);

    RelDataType rowType = root.getRowType();
    assertTrue(rowType.getFieldCount() > 0);
    assertTrue(rowType.getFieldNames().contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));
  }
}
