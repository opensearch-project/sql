/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/** Utility class for expanding dynamic fields in QueryResponse into result columns */
@UtilityClass
public class DynamicFieldsResultProcessor {

  /**
   * Expand dynamic fields into individual columns in QueryResponse.
   *
   * @param response Original QueryResponse with _MAP column
   * @return New QueryResponse with expanded individual columns
   */
  public static QueryResponse expandDynamicFields(QueryResponse response) {
    if (!hasDynamicFields(response)) {
      return response;
    }

    Map<String, ExprType> dynamicFieldTypes = getDynamicFieldTypes(response.getResults());
    Schema expandedSchema = createExpandedSchema(response.getSchema(), dynamicFieldTypes);
    List<ExprValue> expandedRows = expandResultRows(response.getResults(), expandedSchema);

    return new QueryResponse(expandedSchema, expandedRows, response.getCursor());
  }

  private static boolean hasDynamicFields(QueryResponse response) {
    return response.getSchema().getColumns().stream()
        .anyMatch(column -> DYNAMIC_FIELDS_MAP.equals(column.getName()));
  }

  private static Schema createExpandedSchema(
      Schema originalSchema, Map<String, ExprType> dynamicFieldTypes) {
    List<Column> expandedColumns =
        originalSchema.getColumns().stream()
            .filter(col -> !DYNAMIC_FIELDS_MAP.equals(col.getName()))
            .collect(Collectors.toList());
    Set<String> staticFields =
        expandedColumns.stream().map(col -> col.getName()).collect(Collectors.toSet());

    List<String> sortedDynamicFields =
        dynamicFieldTypes.keySet().stream().sorted().collect(Collectors.toList());
    for (String dynamicFieldName : sortedDynamicFields) {
      ExprType fieldType = dynamicFieldTypes.get(dynamicFieldName);
      if (!staticFields.contains(dynamicFieldName)) {
        expandedColumns.add(new Column(dynamicFieldName, null, fieldType));
      }
    }

    return new Schema(expandedColumns);
  }

  /** Expands result rows by extracting MAP field values into individual columns. */
  private static List<ExprValue> expandResultRows(
      List<ExprValue> originalResults, Schema expandedSchema) {
    List<ExprValue> expandedResults = new ArrayList<>();

    for (ExprValue row : originalResults) {
      if (row instanceof ExprTupleValue) {
        expandedResults.add(expandRow((ExprTupleValue) row, expandedSchema));
      } else {
        // Non-tuple rows are passed through unchanged
        expandedResults.add(row);
      }
    }

    return expandedResults;
  }

  private static ExprTupleValue expandRow(ExprTupleValue row, Schema expandedSchema) {
    Map<String, ExprValue> expandedRow = new LinkedHashMap<>();
    Map<String, ExprValue> originalRow = row.tupleValue();
    Map<String, ExprValue> dynamicFields = getDynamicFields(row);

    for (Column column : expandedSchema.getColumns()) {
      String colName = column.getName();
      expandedRow.put(colName, getColValue(originalRow, dynamicFields, colName));
    }
    return ExprTupleValue.fromExprValueMap(expandedRow);
  }

  private static ExprValue getColValue(
      Map<String, ExprValue> originalRow, Map<String, ExprValue> dynamicFields, String colName) {
    if (originalRow.containsKey(colName)) {
      return originalRow.get(colName);
    } else if (dynamicFields.containsKey(colName)) {
      // All the dynamic fields are converted to STRING for type consistency.
      return convertToStringValue(dynamicFields.get(colName));
    } else {
      return ExprValueUtils.nullValue();
    }
  }

  private static ExprValue convertToStringValue(ExprValue v) {
    if (v instanceof ExprStringValue || v instanceof ExprNullValue) {
      return v;
    } else {
      return new ExprStringValue(String.valueOf(v));
    }
  }

  private static Map<String, ExprType> getDynamicFieldTypes(List<ExprValue> results) {
    Set<String> fieldNames = collectDynamicFields(results);
    Map<String, ExprType> inferredTypes = new LinkedHashMap<>();
    for (String fieldName : fieldNames) {
      inferredTypes.put(fieldName, ExprCoreType.STRING);
    }
    return inferredTypes;
  }

  private Set<String> collectDynamicFields(List<ExprValue> results) {
    Set<String> fieldNames = new HashSet<>();

    for (ExprValue row : results) {
      if (row instanceof ExprTupleValue) {
        Map<String, ExprValue> dynamicFields = getDynamicFields(row);
        fieldNames.addAll(dynamicFields.keySet());
      }
    }
    return fieldNames;
  }

  private static Map<String, ExprValue> getDynamicFields(ExprValue row) {
    ExprValue mapValue = row.tupleValue().get(DYNAMIC_FIELDS_MAP);
    if (mapValue == null || mapValue.isNull() || mapValue.isMissing()) {
      return Map.of();
    }

    if (mapValue instanceof ExprTupleValue) {
      return mapValue.tupleValue();
    }

    return Map.of();
  }
}
