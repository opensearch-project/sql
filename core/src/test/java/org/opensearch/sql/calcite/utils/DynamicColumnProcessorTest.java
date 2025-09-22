/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

class DynamicColumnProcessorTest {

  @Test
  void testExpandDynamicColumnsWithMixedTypes() {
    // Create test data with mixed types in dynamic columns
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(1),
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(
                    Map.of(
                        "name", new ExprStringValue("John"),
                        "age", new ExprIntegerValue(30),
                        "score", new ExprIntegerValue(95)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(2),
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(
                    Map.of(
                        "name", new ExprStringValue("Jane"),
                        "age", new ExprIntegerValue(25),
                        "score", new ExprIntegerValue(88)))));

    Schema originalSchema =
        new Schema(
            List.of(
                new Column("id", null, ExprCoreType.INTEGER),
                new Column(
                    DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD, null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);

    // Process the response
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Verify schema expansion
    List<Column> expandedColumns = expandedResponse.getSchema().getColumns();
    assertEquals(4, expandedColumns.size());

    // Check that original columns are preserved (except _dynamic_columns)
    assertEquals("id", expandedColumns.get(0).getName());
    assertEquals(ExprCoreType.INTEGER, expandedColumns.get(0).getExprType());

    assertEquals("age", expandedColumns.get(1).getName());
    assertEquals(ExprCoreType.STRING, expandedColumns.get(1).getExprType());

    assertEquals("name", expandedColumns.get(2).getName());
    assertEquals(ExprCoreType.STRING, expandedColumns.get(2).getExprType());

    assertEquals("score", expandedColumns.get(3).getName());
    assertEquals(ExprCoreType.STRING, expandedColumns.get(3).getExprType());

    List<ExprValue> expandedResults = expandedResponse.getResults();
    assertEquals(2, expandedResults.size());

    ExprTupleValue expandedRow1 = (ExprTupleValue) expandedResults.get(0);
    assertEquals(new ExprIntegerValue(1), expandedRow1.tupleValue().get("id"));
    assertEquals(new ExprStringValue("John"), expandedRow1.tupleValue().get("name"));
    assertEquals(new ExprIntegerValue(30), expandedRow1.tupleValue().get("age"));
    assertEquals(new ExprIntegerValue(95), expandedRow1.tupleValue().get("score"));

    ExprTupleValue expandedRow2 = (ExprTupleValue) expandedResults.get(1);
    assertEquals(new ExprIntegerValue(2), expandedRow2.tupleValue().get("id"));
    assertEquals(new ExprStringValue("Jane"), expandedRow2.tupleValue().get("name"));
    assertEquals(new ExprIntegerValue(25), expandedRow2.tupleValue().get("age"));
    assertEquals(new ExprIntegerValue(88), expandedRow2.tupleValue().get("score"));
  }

  @Test
  void testExpandDynamicColumnsWithMissingValues() {
    // Create test data where some rows have missing dynamic columns
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(1),
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(
                    Map.of(
                        "name", new ExprStringValue("John"),
                        "age", new ExprIntegerValue(30)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(2),
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(
                    Map.of(
                        "name", new ExprStringValue("Jane"),
                        "city", new ExprStringValue("Boston")))));

    Schema originalSchema =
        new Schema(
            List.of(
                new Column("id", null, ExprCoreType.INTEGER),
                new Column(
                    DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD, null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);

    // Process the response
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Verify schema expansion - should include all dynamic columns from all rows
    List<Column> expandedColumns = expandedResponse.getSchema().getColumns();
    assertEquals(4, expandedColumns.size());

    assertEquals("id", expandedColumns.get(0).getName());
    assertEquals("age", expandedColumns.get(1).getName());
    assertEquals("city", expandedColumns.get(2).getName());
    assertEquals("name", expandedColumns.get(3).getName());

    // Verify data expansion with null values for missing columns
    List<ExprValue> expandedResults = expandedResponse.getResults();
    assertEquals(2, expandedResults.size());

    // Check first row - should have null for city
    ExprTupleValue expandedRow1 = (ExprTupleValue) expandedResults.get(0);
    assertEquals(new ExprIntegerValue(1), expandedRow1.tupleValue().get("id"));
    assertEquals(new ExprStringValue("John"), expandedRow1.tupleValue().get("name"));
    assertEquals(new ExprIntegerValue(30), expandedRow1.tupleValue().get("age"));
    assertTrue(expandedRow1.tupleValue().get("city").isNull());

    // Check second row - should have null for age
    ExprTupleValue expandedRow2 = (ExprTupleValue) expandedResults.get(1);
    assertEquals(new ExprIntegerValue(2), expandedRow2.tupleValue().get("id"));
    assertEquals(new ExprStringValue("Jane"), expandedRow2.tupleValue().get("name"));
    assertTrue(expandedRow2.tupleValue().get("age").isNull());
    assertEquals(new ExprStringValue("Boston"), expandedRow2.tupleValue().get("city"));
  }

  @Test
  void testExpandDynamicColumnsWithNoDynamicColumns() {
    // Create test data without dynamic columns
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id", new ExprIntegerValue(1),
                "name", new ExprStringValue("John")));

    Schema originalSchema =
        new Schema(
            List.of(
                new Column("id", null, ExprCoreType.INTEGER),
                new Column("name", null, ExprCoreType.STRING)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1), null);

    // Process the response
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should return the original response unchanged
    assertEquals(originalResponse, expandedResponse);
  }

  @Test
  void testInferDynamicColumnTypeWithAllNullValues() {
    // Test type inference with all null values - should default to STRING
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(Map.of("nullable", ExprValueUtils.nullValue()))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD,
                ExprTupleValue.fromExprValueMap(Map.of())));

    Schema originalSchema =
        new Schema(
            List.of(
                new Column(
                    DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD, null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should default to STRING type for all-null columns
    Column nullableColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("nullable", nullableColumn.getName());
    assertEquals(ExprCoreType.STRING, nullableColumn.getExprType());
  }
}
