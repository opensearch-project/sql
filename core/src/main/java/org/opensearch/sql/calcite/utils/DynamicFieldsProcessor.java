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
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/** Utility class for processing dynamic fields in query results. */
@UtilityClass
public class DynamicFieldsProcessor {
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

    Map<String, ExprType> dynamicFieldTypes = inferDynamicFieldTypes(response.getResults());
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

    for (Map.Entry<String, ExprType> dynamicFieldType : dynamicFieldTypes.entrySet()) {
      if (!staticFields.contains(dynamicFieldType.getKey())) {
        expandedColumns.add(
            new Column(dynamicFieldType.getKey(), null, dynamicFieldType.getValue()));
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
        Map<String, ExprValue> expandedRow = new LinkedHashMap<>();
        Map<String, ExprValue> originalRow = row.tupleValue();
        Map<String, ExprValue> dynamicFields = getDynamicFields(row);

        for (Column column : expandedSchema.getColumns()) {
          String colName = column.getName();
          if (originalRow.containsKey(colName)) {
            expandedRow.put(colName, originalRow.get(colName));
          } else if (dynamicFields.containsKey(colName)) {
            expandedRow.put(colName, dynamicFields.get(colName));
          } else {
            expandedRow.put(colName, ExprValueUtils.nullValue());
          }
        }

        expandedResults.add(ExprTupleValue.fromExprValueMap(expandedRow));
      } else {
        // Non-tuple rows are passed through unchanged
        expandedResults.add(row);
      }
    }

    return expandedResults;
  }

  /**
   * Infers the type for a dynamic field by analyzing values from the MAP across all result rows.
   *
   * @param results All result rows containing dynamic fields MAP
   * @return dynamic field name to inferred type MAP
   */
  public static Map<String, ExprType> inferDynamicFieldTypes(List<ExprValue> results) {
    Map<String, Set<ExprType>> foundTypes = collectDynamicFieldTypes(results);
    Map<String, ExprType> inferredTypes = new LinkedHashMap<>();
    for (Map.Entry<String, Set<ExprType>> entry : foundTypes.entrySet()) {
      inferredTypes.put(entry.getKey(), inferCommonType(entry.getValue()));
    }
    return inferredTypes;
  }

  private Map<String, Set<ExprType>> collectDynamicFieldTypes(List<ExprValue> results) {
    Map<String, Set<ExprType>> fieldNameToTypeSet = new LinkedHashMap<>();

    for (ExprValue row : results) {
      if (row instanceof ExprTupleValue) {
        Map<String, ExprValue> dynamicFields = getDynamicFields(row);
        for (String fieldName : dynamicFields.keySet()) {
          ExprValue fieldValue = dynamicFields.get(fieldName);
          if (!fieldNameToTypeSet.containsKey(fieldName)) {
            fieldNameToTypeSet.put(fieldName, new HashSet<>());
          }
          fieldNameToTypeSet.get(fieldName).add(fieldValue.type());
        }
      }
    }
    return fieldNameToTypeSet;
  }

  /**
   * Infer field type from the column values in the results. It is used for columns derived from
   * dynamic fields.
   */
  public ExprType inferColumnType(List<ExprValue> results, String columnName) {
    Set<ExprType> typeSet = collectFieldTypes(results, columnName);
    return inferCommonType(typeSet);
  }

  private Set<ExprType> collectFieldTypes(List<ExprValue> results, String columnName) {
    Set<ExprType> typeSet = new HashSet<>();

    for (ExprValue row : results) {
      if (row instanceof ExprTupleValue) {
        typeSet.add(row.tupleValue().get(columnName).type());
      }
    }
    return typeSet;
  }

  public ExprType inferCommonType(Set<ExprType> types) {
    if (types.isEmpty()) {
      return ExprCoreType.UNDEFINED;
    }
    if (types.size() == 1) {
      return types.iterator().next();
    }
    if (types.contains(ExprCoreType.STRING)) {
      return ExprCoreType.STRING; // String can represent any value
    }
    if (types.contains(ExprCoreType.DOUBLE)) {
      return ExprCoreType.DOUBLE; // Double can represent integers
    }
    if (types.contains(ExprCoreType.LONG)) {
      return ExprCoreType.LONG; // Long can represent integers
    }
    if (types.contains(ExprCoreType.INTEGER)) {
      return ExprCoreType.INTEGER;
    }
    if (types.contains(ExprCoreType.BOOLEAN)) {
      return ExprCoreType.BOOLEAN;
    }
    return ExprCoreType.UNDEFINED;
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
