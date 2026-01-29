/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.pagination.Cursor;

class DynamicFieldsResultProcessorTest {

  @Test
  void expandDynamicFields_withMultipleRowsAndFields() {
    QueryResponse response =
        createResponseWithDynamicFields(
            List.of(
                createRow(1, "Alice", Map.of("age", intVal(30), "city", strVal("NYC"))),
                createRow(2, "Bob", Map.of("age", intVal(25), "country", strVal("USA")))));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name", "age", "city", "country");
    assertDynamicFieldsAreStrings(expanded, "age", "city", "country");

    assertRowValues(
        expanded,
        0,
        Map.of(
            "id",
            intVal(1),
            "name",
            strVal("Alice"),
            "age",
            strVal("30"),
            "city",
            strVal("NYC"),
            "country",
            nullVal()));
    assertRowValues(
        expanded,
        1,
        Map.of(
            "id",
            intVal(2),
            "name",
            strVal("Bob"),
            "age",
            strVal("25"),
            "city",
            nullVal(),
            "country",
            strVal("USA")));
  }

  @Test
  void expandDynamicFields_returnsOriginalResponse_whenNoDynamicFieldsMap() {
    Schema schema =
        createSchema(List.of(col("id", ExprCoreType.INTEGER), col("name", ExprCoreType.STRING)));
    List<ExprValue> results =
        List.of(createTuple(Map.of("id", intVal(1), "name", strVal("Alice"))));
    QueryResponse response = new QueryResponse(schema, results, null);

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSame(response, expanded);
  }

  @Test
  void expandDynamicFields_handlesNullMapValue() {
    QueryResponse response = createResponseWithDynamicFields(List.of(createRowWithNullMap(1)));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name");
    assertRowValues(expanded, 0, Map.of("id", intVal(1), "name", nullVal()));
  }

  @Test
  void expandDynamicFields_handlesEmptyMapValue() {
    QueryResponse response =
        createResponseWithDynamicFields(List.of(createRow(1, "Alice", Map.of())));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name");
  }

  @Test
  void expandDynamicFields_staticFieldsTakePrecedence_overDynamicFields() {
    Schema schema = createSchemaWithStaticAge();
    Map<String, ExprValue> row = new LinkedHashMap<>();
    row.put("id", intVal(1));
    row.put("age", intVal(30));
    row.put(DYNAMIC_FIELDS_MAP, createTuple(Map.of("age", intVal(99), "city", strVal("NYC"))));

    QueryResponse response = new QueryResponse(schema, List.of(createTuple(row)), null);
    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "age", "city");
    assertEquals(ExprCoreType.INTEGER, getColumnType(expanded, "age"));
    assertRowValues(expanded, 0, Map.of("id", intVal(1), "age", intVal(30), "city", strVal("NYC")));
  }

  @Test
  void expandDynamicFields_convertsNonStringValues_toString() {
    QueryResponse response =
        createResponseWithDynamicFields(
            List.of(
                createRow(
                    1,
                    "Alice",
                    Map.of(
                        "intField", intVal(42),
                        "boolField", ExprBooleanValue.of(true),
                        "stringField", strVal("text"),
                        "nullField", nullVal()))));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertRowValues(
        expanded,
        0,
        Map.of(
            "intField", strVal("42"),
            "boolField", strVal("true"),
            "stringField", strVal("text"),
            "nullField", nullVal()));
  }

  @Test
  void expandDynamicFields_sortsDynamicFields_alphabetically() {
    QueryResponse response =
        createResponseWithDynamicFields(
            List.of(
                createRow(
                    1,
                    "Alice",
                    Map.of(
                        "zebra", strVal("z"),
                        "apple", strVal("a"),
                        "mango", strVal("m"),
                        "banana", strVal("b")))));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name", "apple", "banana", "mango", "zebra");
  }

  @Test
  void expandDynamicFields_mergesDynamicFields_acrossMultipleRows() {
    QueryResponse response =
        createResponseWithDynamicFields(
            List.of(
                createRow(1, "Alice", Map.of("field1", strVal("v1"), "field2", strVal("v2"))),
                createRow(2, "Bob", Map.of("field2", strVal("v2b"), "field3", strVal("v3"))),
                createRowWithNullMap(3)));

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name", "field1", "field2", "field3");
    assertRowValues(
        expanded,
        0,
        Map.of(
            "id",
            intVal(1),
            "name",
            strVal("Alice"),
            "field1",
            strVal("v1"),
            "field2",
            strVal("v2"),
            "field3",
            nullVal()));
    assertRowValues(
        expanded,
        1,
        Map.of(
            "id",
            intVal(2),
            "name",
            strVal("Bob"),
            "field1",
            nullVal(),
            "field2",
            strVal("v2b"),
            "field3",
            strVal("v3")));
    assertRowValues(
        expanded,
        2,
        Map.of(
            "id", intVal(3), "name", nullVal(), "field1", nullVal(), "field2", nullVal(), "field3",
            nullVal()));
  }

  @Test
  void expandDynamicFields_handlesEmptyResults() {
    Schema schema = createSchemaWithDynamicFields();
    QueryResponse response = new QueryResponse(schema, List.of(), null);

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertSchemaColumns(expanded, "id", "name");
    assertEquals(0, expanded.getResults().size());
  }

  @Test
  void expandDynamicFields_preservesCursor() {
    Cursor cursor = new Cursor("test-cursor");
    QueryResponse response =
        createResponseWithDynamicFields(List.of(createRow(1, "Alice", Map.of())), cursor);

    QueryResponse expanded = DynamicFieldsResultProcessor.expandDynamicFields(response);

    assertEquals(cursor, expanded.getCursor());
  }

  private QueryResponse createResponseWithDynamicFields(List<Map<String, ExprValue>> rows) {
    return createResponseWithDynamicFields(rows, null);
  }

  private QueryResponse createResponseWithDynamicFields(
      List<Map<String, ExprValue>> rows, Cursor cursor) {
    Schema schema = createSchemaWithDynamicFields();
    List<ExprValue> results =
        rows.stream().map(this::createTuple).map(tuple -> (ExprValue) tuple).toList();
    return new QueryResponse(schema, results, cursor);
  }

  private Map<String, ExprValue> createRow(
      int id, String name, Map<String, ExprValue> dynamicFields) {
    Map<String, ExprValue> row = new LinkedHashMap<>();
    row.put("id", intVal(id));
    row.put("name", strVal(name));
    row.put(DYNAMIC_FIELDS_MAP, createTuple(dynamicFields));
    return row;
  }

  private Map<String, ExprValue> createRowWithNullMap(int id) {
    Map<String, ExprValue> row = new LinkedHashMap<>();
    row.put("id", intVal(id));
    row.put("name", nullVal());
    row.put(DYNAMIC_FIELDS_MAP, nullVal());
    return row;
  }

  private Schema createSchemaWithDynamicFields() {
    return createSchema(
        List.of(
            col("id", ExprCoreType.INTEGER),
            col("name", ExprCoreType.STRING),
            col(DYNAMIC_FIELDS_MAP, ExprCoreType.STRUCT)));
  }

  private Schema createSchemaWithStaticAge() {
    return createSchema(
        List.of(
            col("id", ExprCoreType.INTEGER),
            col("age", ExprCoreType.INTEGER),
            col(DYNAMIC_FIELDS_MAP, ExprCoreType.STRUCT)));
  }

  private Schema createSchema(List<Column> columns) {
    return new Schema(columns);
  }

  private Column col(String name, ExprType type) {
    return new Column(name, null, type);
  }

  private ExprTupleValue createTuple(Map<String, ExprValue> values) {
    return ExprTupleValue.fromExprValueMap(values);
  }

  private ExprIntegerValue intVal(int value) {
    return new ExprIntegerValue(value);
  }

  private ExprStringValue strVal(String value) {
    return new ExprStringValue(value);
  }

  private ExprNullValue nullVal() {
    return ExprNullValue.of();
  }

  private void assertSchemaColumns(QueryResponse response, String... expectedColumns) {
    assertNotNull(response);
    List<Column> columns = response.getSchema().getColumns();
    assertEquals(expectedColumns.length, columns.size());
    for (int i = 0; i < expectedColumns.length; i++) {
      assertEquals(expectedColumns[i], columns.get(i).getName());
    }
  }

  private void assertDynamicFieldsAreStrings(QueryResponse response, String... fieldNames) {
    for (String fieldName : fieldNames) {
      assertEquals(ExprCoreType.STRING, getColumnType(response, fieldName));
    }
  }

  private ExprType getColumnType(QueryResponse response, String columnName) {
    return response.getSchema().getColumns().stream()
        .filter(col -> col.getName().equals(columnName))
        .findFirst()
        .map(Column::getExprType)
        .orElseThrow(() -> new AssertionError("Column not found: " + columnName));
  }

  private void assertRowValues(
      QueryResponse response, int rowIndex, Map<String, ExprValue> expectedValues) {
    ExprTupleValue row = (ExprTupleValue) response.getResults().get(rowIndex);
    Map<String, ExprValue> actualValues = row.tupleValue();

    for (Map.Entry<String, ExprValue> entry : expectedValues.entrySet()) {
      String fieldName = entry.getKey();
      ExprValue expectedValue = entry.getValue();
      ExprValue actualValue = actualValues.get(fieldName);

      assertEquals(expectedValue, actualValue);
    }
  }
}
