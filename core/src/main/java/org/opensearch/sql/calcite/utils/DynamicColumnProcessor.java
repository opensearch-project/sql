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
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
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
   * Convert dynamic columns in the QueryResponse into individual columns.
   *
   * @param response Original QueryResponse with MAP fields
   * @return New QueryResponse with expanded individual columns
   */
  public static QueryResponse expandDynamicColumns(QueryResponse response) {
    if (!hasDynamicColumns(response)) {
      return response;
    }

    Set<String> dynamicColumnNames = collectDynamicColumnNames(response.getResults());
    Schema expandedSchema =
        createExpandedSchema(response.getSchema(), dynamicColumnNames, response.getResults());
    List<ExprValue> expandedResults =
        expandResultRowsWithDynamicColumns(response.getResults(), dynamicColumnNames);

    return new QueryResponse(expandedSchema, expandedResults, response.getCursor());
  }

  /**
   * Ensures that the dynamic columns field exists in the schema if needed. Adds a NULL MAP field
   * for _dynamic_columns if it doesn't exist and dynamic columns are needed.
   *
   * @param context CalcitePlanContext containing the RelBuilder and other context
   */
  public static void ensureDynamicColumnsFieldExists(CalcitePlanContext context) {
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (!currentFields.contains(DYNAMIC_COLUMNS_FIELD)) {
      // Add NULL MAP field for _dynamic_columns if it doesn't exist
      // This creates a placeholder that map_merge can work with
      RexNode nullMapField =
          context.rexBuilder.makeNullLiteral(
              context
                  .rexBuilder
                  .getTypeFactory()
                  .createMapType(
                      context
                          .rexBuilder
                          .getTypeFactory()
                          .createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
                      context
                          .rexBuilder
                          .getTypeFactory()
                          .createSqlType(org.apache.calcite.sql.type.SqlTypeName.ANY)));
      RexNode dynamicColumnsField = context.relBuilder.alias(nullMapField, DYNAMIC_COLUMNS_FIELD);
      context.relBuilder.projectPlus(dynamicColumnsField);

      // Mark that dynamic columns are now available
      context.setDynamicColumnsAvailable(true);
    }
  }

  /**
   * Checks if the response contains dynamic columns (MAP fields with DYNAMIC_COLUMNS_FIELD name).
   */
  private static boolean hasDynamicColumns(QueryResponse response) {
    return response.getSchema().getColumns().stream()
        .anyMatch(column -> DYNAMIC_COLUMNS_FIELD.equals(column.getName()));
  }

  private static Set<String> collectDynamicColumnNames(List<ExprValue> results) {
    Set<String> columnNames = new TreeSet<>(); // Use TreeSet for consistent ordering

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

  /** It will deduplicate columns if dynamic column name conflicts with original schema */
  private static Schema createExpandedSchema(
      Schema originalSchema, Set<String> dynamicColumnNames, List<ExprValue> results) {
    Set<String> originalColumnNames = collectColumnNames(originalSchema);
    List<Column> expandedColumns = getColumnsExcludingDynamic(originalSchema);

    for (String columnName : dynamicColumnNames) {
      if (!originalColumnNames.contains(columnName)) {
        expandedColumns.add(new Column(columnName, null, ExprCoreType.STRING));
      }
    }

    return new Schema(expandedColumns);
  }

  private static Set<String> collectColumnNames(Schema schema) {
    return schema.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
  }

  private static List<Column> getColumnsExcludingDynamic(Schema originalSchema) {
    return originalSchema.getColumns().stream()
        .filter(c -> !DYNAMIC_COLUMNS_FIELD.equals(c.getName()))
        .collect(Collectors.toList());
  }

  private static List<ExprValue> expandResultRowsWithDynamicColumns(
      List<ExprValue> originalResults, Set<String> dynamicColumnNames) {
    List<ExprValue> expandedResults = new ArrayList<>();

    for (ExprValue row : originalResults) {
      if (row instanceof ExprTupleValue) {
        Map<String, ExprValue> originalRow = row.tupleValue();
        Map<String, ExprValue> expandedRow = copyExceptDynamic(originalRow);

        Map<String, ExprValue> dynamicColumns =
            extractMapValue(originalRow.get(DYNAMIC_COLUMNS_FIELD));

        // This will overwrite existing columns if there's a name conflict
        for (String columnName : dynamicColumnNames) {
          ExprValue columnValue = dynamicColumns.get(columnName);
          if (columnValue != null) {
            expandedRow.put(columnName, columnValue);
          } else {
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

  private static Map<String, ExprValue> copyExceptDynamic(Map<String, ExprValue> originalRow) {
    Map<String, ExprValue> copiedRow = new LinkedHashMap<>();
    for (Map.Entry<String, ExprValue> entry : originalRow.entrySet()) {
      if (!DYNAMIC_COLUMNS_FIELD.equals(entry.getKey())) {
        copiedRow.put(entry.getKey(), entry.getValue());
      }
    }
    return copiedRow;
  }

  private static Map<String, ExprValue> extractMapValue(ExprValue mapValue) {
    if (mapValue == null || mapValue.isNull() || mapValue.isMissing()) {
      return Map.of();
    }

    // If it's a tuple value, treat it as a map
    if (mapValue instanceof ExprTupleValue) {
      return mapValue.tupleValue();
    }

    // TODO: Handle other MAP representations if needed
    // For now, return empty map for unsupported types
    return Map.of();
  }

  /**
   * Checks if a field should be resolved as a dynamic column access. This happens when: 1. The
   * field is not found in the current schema 2. Dynamic columns are available (marked by the
   * context) 3. We're not in a coalesce function (which has special null handling)
   */
  public static boolean tryResolveDynamicField(
      String fieldName, List<String> currentFields, CalcitePlanContext context) {
    // Don't resolve dynamic fields in coalesce function (it has special null handling)
    if (context.isInCoalesceFunction()) {
      return false;
    }

    // Check if field is not in current schema
    if (currentFields.contains(fieldName)) {
      return false;
    }

    // CRITICAL FIX: Check if _dynamic_columns field exists in current schema
    // This is more reliable than the flag approach
    boolean hasDynamicColumnsField =
        currentFields.contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);

    // Check if dynamic columns are available (either by flag or by field presence)
    boolean hasDynamicColumns = context.isDynamicColumnsAvailable() || hasDynamicColumnsField;

    return hasDynamicColumns;
  }

  /**
   * Resolves a field as dynamic column access by rewriting it to MAP access. Converts: fieldName ->
   * _dynamic_columns['fieldName']
   *
   * <p>CONTEXT-AWARE FIELD RESOLUTION: - For fields command: Apply aliasing to preserve field names
   * - For GROUP BY context: Apply VARCHAR casting to avoid UNDEFINED type issues - For other
   * commands: Use direct MAP access preserving original types
   */
  public static RexNode resolveDynamicField(String fieldName, CalcitePlanContext context) {
    System.out.println("=== DEBUG resolveDynamicField === fieldName=" + fieldName);
    // Access the _dynamic_columns MAP field
    RexNode dynamicColumnsField =
        context.relBuilder.field(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);

    // Create MAP access: _dynamic_columns[fieldName]
    RexNode mapAccess =
        MapAccessOperations.mapGet(context.rexBuilder, dynamicColumnsField, fieldName);

    // SELECTIVE VARCHAR CASTING: Only apply in GROUP BY contexts to avoid UNDEFINED type issues
    // For other contexts, preserve original types (int, double, etc.) for better type inference
    RexNode finalMapAccess;
    if (context.isInGroupByContext()) {
      finalMapAccess =
          context.rexBuilder.makeCast(
              context
                  .rexBuilder
                  .getTypeFactory()
                  .createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
              mapAccess);
    } else {
      finalMapAccess = mapAccess;
    }

    // CONDITIONAL ALIASING: Apply aliasing when in fields command context
    if (context.isInFieldsCommand()) {
      return context.relBuilder.alias(finalMapAccess, fieldName);
    } else {
      return finalMapAccess;
    }
  }
}
