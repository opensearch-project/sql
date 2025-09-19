/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

class DynamicColumnProcessorAdvancedTest {

  @Test
  void testInferDynamicColumnTypeWithNumericPrecedence() {
    // Test type precedence: DOUBLE > LONG > INTEGER
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(
                    Map.of("numeric_value", new ExprIntegerValue(10)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("numeric_value", new ExprLongValue(20L)))));

    ExprValue row3 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(
                    Map.of("numeric_value", new ExprDoubleValue(30.5)))));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse =
        new QueryResponse(originalSchema, List.of(row1, row2, row3), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should infer DOUBLE type due to precedence rules
    Column numericColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("numeric_value", numericColumn.getName());
    assertEquals(ExprCoreType.DOUBLE, numericColumn.getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithLongIntegerPrecedence() {
    // Test type precedence: LONG > INTEGER
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("number", new ExprIntegerValue(10)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("number", new ExprLongValue(20L)))));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should infer LONG type due to precedence rules
    Column numberColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("number", numberColumn.getName());
    assertEquals(ExprCoreType.LONG, numberColumn.getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithBooleanValues() {
    // Test boolean type inference
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("flag", ExprBooleanValue.of(true)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("flag", ExprBooleanValue.of(false)))));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should infer BOOLEAN type
    Column flagColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("flag", flagColumn.getName());
    assertEquals(ExprCoreType.BOOLEAN, flagColumn.getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithStringBooleanPrecedence() {
    // Test type precedence: STRING > BOOLEAN
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("mixed_value", ExprBooleanValue.of(true)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(
                    Map.of("mixed_value", new ExprStringValue("not_boolean")))));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should infer STRING type due to precedence rules
    Column mixedColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("mixed_value", mixedColumn.getName());
    assertEquals(ExprCoreType.STRING, mixedColumn.getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithComplexMixedTypes() {
    // Test complex scenario with all types mixed
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("complex", new ExprIntegerValue(10)))));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("complex", new ExprLongValue(20L)))));

    ExprValue row3 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("complex", new ExprDoubleValue(30.5)))));

    ExprValue row4 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("complex", ExprBooleanValue.of(true)))));

    ExprValue row5 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of("complex", new ExprStringValue("text")))));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse =
        new QueryResponse(originalSchema, List.of(row1, row2, row3, row4, row5), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should infer STRING type due to highest precedence
    Column complexColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("complex", complexColumn.getName());
    assertEquals(ExprCoreType.STRING, complexColumn.getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithEmptyDynamicColumns() {
    // Test with rows that have empty _dynamic_columns maps
    ExprValue row1 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(1),
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of())));

    ExprValue row2 =
        ExprTupleValue.fromExprValueMap(
            Map.of(
                "id",
                new ExprIntegerValue(2),
                "_dynamic_columns",
                ExprTupleValue.fromExprValueMap(Map.of())));

    Schema originalSchema =
        new Schema(
            List.of(
                new Column("id", null, ExprCoreType.INTEGER),
                new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, List.of(row1, row2), null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should only have the original id column, no dynamic columns
    List<Column> expandedColumns = expandedResponse.getSchema().getColumns();
    assertEquals(1, expandedColumns.size());
    assertEquals("id", expandedColumns.get(0).getName());
    assertEquals(ExprCoreType.INTEGER, expandedColumns.get(0).getExprType());
  }

  @Test
  void testInferDynamicColumnTypeWithLargeDataset() {
    // Test performance and correctness with larger dataset
    List<ExprValue> rows =
        List.of(
            createRowWithDynamicColumn("value", new ExprIntegerValue(1)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(2)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(3)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(4)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(5)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(6)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(7)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(8)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(9)),
            createRowWithDynamicColumn("value", new ExprIntegerValue(10)));

    Schema originalSchema =
        new Schema(List.of(new Column("_dynamic_columns", null, ExprCoreType.STRUCT)));

    QueryResponse originalResponse = new QueryResponse(originalSchema, rows, null);
    QueryResponse expandedResponse = DynamicColumnProcessor.expandDynamicColumns(originalResponse);

    // Should correctly infer INTEGER type even with many rows
    Column valueColumn = expandedResponse.getSchema().getColumns().get(0);
    assertEquals("value", valueColumn.getName());
    assertEquals(ExprCoreType.INTEGER, valueColumn.getExprType());

    // Verify all rows are processed correctly
    assertEquals(10, expandedResponse.getResults().size());
  }

  private ExprValue createRowWithDynamicColumn(String columnName, ExprValue columnValue) {
    return ExprTupleValue.fromExprValueMap(
        Map.of(
            "_dynamic_columns", ExprTupleValue.fromExprValueMap(Map.of(columnName, columnValue))));
  }
}
