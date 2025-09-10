/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Utility class for processing dynamic columns in query results. Handles expansion of MAP fields
 * containing dynamic columns into individual columns.
 */
@UtilityClass
public class DynamicColumnProcessor {

  /** Field name used for storing dynamic columns as MAP */
  public static final String DYNAMIC_COLUMNS_FIELD = "_dynamic_columns";

  /**
   * Processes a QueryResponse to expand dynamic columns from MAP fields into individual columns.
   *
   * @param response Original QueryResponse with MAP fields
   * @return New QueryResponse with expanded individual columns
   */
  public static QueryResponse expandDynamicColumns(QueryResponse response) {
    System.out.println("=== DEBUG DynamicColumnProcessor.expandDynamicColumns ===");
    System.out.println(
        "Original schema columns: "
            + response.getSchema().getColumns().stream()
                .map(col -> col.getName() + ":" + col.getExprType())
                .toList());
    System.out.println("Number of result rows: " + response.getResults().size());

    if (!hasDynamicColumns(response)) {
      System.out.println("No dynamic columns found, returning original response");
      return response;
    }

    System.out.println("Dynamic columns detected, processing...");

    // Collect all dynamic column names from all rows
    Set<String> dynamicColumnNames = collectDynamicColumnNames(response.getResults());

    // Create new schema with expanded columns
    Schema expandedSchema = createExpandedSchema(response.getSchema(), dynamicColumnNames);

    // Transform results to expand MAP fields into individual columns
    List<ExprValue> expandedResults = expandResultRows(response.getResults(), dynamicColumnNames);

    return new QueryResponse(expandedSchema, expandedResults, response.getCursor());
  }

  /**
   * Checks if the response contains dynamic columns (MAP fields with DYNAMIC_COLUMNS_FIELD name).
   */
  private static boolean hasDynamicColumns(QueryResponse response) {
    return response.getSchema().getColumns().stream()
        .anyMatch(column -> DYNAMIC_COLUMNS_FIELD.equals(column.getName()));
  }

  /** Collects all unique dynamic column names from MAP fields across all result rows. */
  private static Set<String> collectDynamicColumnNames(List<ExprValue> results) {
    Set<String> columnNames = new TreeSet<>(); // TreeSet for consistent ordering

    for (ExprValue row : results) {
      if (row instanceof ExprTupleValue) {
        ExprValue dynamicColumnsValue = row.tupleValue().get(DYNAMIC_COLUMNS_FIELD);
        if (dynamicColumnsValue != null
            && !dynamicColumnsValue.isNull()
            && !dynamicColumnsValue.isMissing()) {
          // Extract keys from the MAP
          Map<String, ExprValue> mapValue = extractMapValue(dynamicColumnsValue);
          columnNames.addAll(mapValue.keySet());
        }
      }
    }

    return columnNames;
  }

  /** Creates a new schema with dynamic columns expanded as individual columns. */
  private static Schema createExpandedSchema(
      Schema originalSchema, Set<String> dynamicColumnNames) {
    List<Column> expandedColumns = new ArrayList<>();

    // Add all original columns except the dynamic columns MAP field
    for (Column column : originalSchema.getColumns()) {
      if (!DYNAMIC_COLUMNS_FIELD.equals(column.getName())) {
        expandedColumns.add(column);
      }
    }

    // Add individual columns for each dynamic column name
    for (String columnName : dynamicColumnNames) {
      // Use STRING type for dynamic columns since JSON values are typically strings
      // TODO: Could enhance this to infer actual types from the data
      expandedColumns.add(new Column(columnName, null, ExprCoreType.STRING));
    }

    return new Schema(expandedColumns);
  }

  /** Expands result rows by extracting MAP field values into individual columns. */
  private static List<ExprValue> expandResultRows(
      List<ExprValue> originalResults, Set<String> dynamicColumnNames) {
    List<ExprValue> expandedResults = new ArrayList<>();

    for (ExprValue row : originalResults) {
      if (row instanceof ExprTupleValue) {
        Map<String, ExprValue> expandedRow = new LinkedHashMap<>();
        Map<String, ExprValue> originalRow = row.tupleValue();

        // Copy all original fields except the dynamic columns MAP field
        for (Map.Entry<String, ExprValue> entry : originalRow.entrySet()) {
          if (!DYNAMIC_COLUMNS_FIELD.equals(entry.getKey())) {
            expandedRow.put(entry.getKey(), entry.getValue());
          }
        }

        // Extract dynamic columns from MAP field
        ExprValue dynamicColumnsValue = originalRow.get(DYNAMIC_COLUMNS_FIELD);
        Map<String, ExprValue> dynamicColumns = extractMapValue(dynamicColumnsValue);

        // Add individual columns for each dynamic column name
        for (String columnName : dynamicColumnNames) {
          ExprValue columnValue = dynamicColumns.get(columnName);
          if (columnValue != null) {
            expandedRow.put(columnName, columnValue);
          } else {
            // Use NULL for missing dynamic columns
            expandedRow.put(columnName, ExprValueUtils.nullValue());
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
   * Extracts a Map from an ExprValue that represents a MAP field. Handles both actual MAP values
   * and NULL/missing values.
   */
  private static Map<String, ExprValue> extractMapValue(ExprValue mapValue) {
    if (mapValue == null || mapValue.isNull() || mapValue.isMissing()) {
      return new LinkedHashMap<>();
    }

    // If it's a tuple value, treat it as a map
    if (mapValue instanceof ExprTupleValue) {
      return mapValue.tupleValue();
    }

    // TODO: Handle other MAP representations if needed
    // For now, return empty map for unsupported types
    return new LinkedHashMap<>();
  }
}
