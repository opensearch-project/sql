/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * JSON response formatter specifically for timechart command. It transforms the data from
 * [timestamp, field, value] format to a pivot table format: [timestamp, field1_value, field2_value,
 * ...].
 */
public class TimechartResponseFormatter extends JsonResponseFormatter<QueryResult> {

  /**
   * Default maximum number of distinct values to display in the timechart. Values beyond this limit
   * will be grouped into an "OTHER" category.
   */
  private static final int DEFAULT_MAX_DISTINCT_VALUES = 10;

  /** Constant for the "OTHER" category name. */
  private static final String OTHER_CATEGORY = "OTHER";

  private final Integer maxDistinctValues;
  private final Boolean useOther;
  private boolean isCountAggregation;

  public TimechartResponseFormatter(Style style) {
    this(style, null, true);
  }

  public TimechartResponseFormatter(Style style, Integer maxDistinctValues) {
    this(style, maxDistinctValues, true);
  }

  public TimechartResponseFormatter(Style style, Integer maxDistinctValues, Boolean useOther) {
    super(style);
    this.maxDistinctValues =
        maxDistinctValues != null ? maxDistinctValues : DEFAULT_MAX_DISTINCT_VALUES;
    this.useOther = useOther != null ? useOther : true;
    this.isCountAggregation = false;
  }

  /**
   * Set whether this formatter is handling a count aggregation. When true, null values will be
   * replaced with 0 in the response.
   *
   * @param isCountAggregation true if the aggregation function is count()
   * @return this formatter instance for method chaining
   */
  public TimechartResponseFormatter withCountAggregation(boolean isCountAggregation) {
    this.isCountAggregation = isCountAggregation;
    return this;
  }

  @Override
  public Object buildJsonObject(QueryResult response) {
    // Check if this is a timechart result
    Map<String, String> columnTypes = response.columnNameTypes();
    List<String> columnNames = new ArrayList<>(columnTypes.keySet());

    // If there are only 2 columns, it's a timechart without 'by' field
    if (columnNames.size() == 2) {
      return buildSimpleJsonObject(response);
    }

    // For timechart with 'by' field, we need to pivot the data
    if (columnNames.size() == 3) {
      return buildPivotJsonObject(response, columnNames);
    }

    // Default to simple JSON format for other cases
    return buildSimpleJsonObject(response);
  }

  private Object buildSimpleJsonObject(QueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();

    json.total(response.size()).size(response.size());

    response.columnNameTypes().forEach((name, type) -> json.column(new Column(name, type)));

    json.datarows(fetchDataRows(response));
    return json.build();
  }

  private Object buildPivotJsonObject(QueryResult response, List<String> columnNames) {
    String timeField = columnNames.get(0);
    String byField = columnNames.get(1);
    String valueField = columnNames.get(2);

    PivotData pivotData = collectPivotData(response);
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    json.column(new Column(timeField, response.columnNameTypes().get(timeField)));

    boolean needsOtherCategory =
        maxDistinctValues > 0 && pivotData.distinctByValues.size() > maxDistinctValues && useOther;

    if (needsOtherCategory) {
      buildResponseWithOtherCategory(json, pivotData, response, valueField);
    } else {
      buildResponseWithoutOtherCategory(json, pivotData, response, valueField);
    }

    return json.build();
  }

  private PivotData collectPivotData(QueryResult response) {
    Map<Object, Map<Object, Object>> pivotData = new TreeMap<>();
    Map<Object, Boolean> distinctByValues = new LinkedHashMap<>();
    Map<Object, Double> valueScores = new HashMap<>();

    for (Object[] row : response) {
      Object timeValue = row[0];
      Object byValue = row[1];
      Object value = row[2];

      distinctByValues.put(byValue, true);
      valueScores.merge(byValue, convertToDouble(value), Double::sum);
      pivotData.computeIfAbsent(timeValue, k -> new HashMap<>()).put(byValue, value);
    }

    return new PivotData(pivotData, distinctByValues, valueScores);
  }

  private void buildResponseWithOtherCategory(
      JsonResponse.JsonResponseBuilder json,
      PivotData pivotData,
      QueryResult response,
      String valueField) {
    List<Object> topValues = getTopValuesInOrder(pivotData, maxDistinctValues);

    // Add columns
    for (Object byValue : topValues) {
      json.column(new Column(byValue.toString(), response.columnNameTypes().get(valueField)));
    }
    json.column(new Column(OTHER_CATEGORY, response.columnNameTypes().get(valueField)));

    // Build data rows
    List<Object[]> dataRows = buildDataRowsWithOther(pivotData.pivotData, topValues);
    json.total(dataRows.size()).size(dataRows.size()).datarows(dataRows.toArray(new Object[0][]));
  }

  private void buildResponseWithoutOtherCategory(
      JsonResponse.JsonResponseBuilder json,
      PivotData pivotData,
      QueryResult response,
      String valueField) {
    List<Object> valuesToShow =
        maxDistinctValues == 0
            ? new ArrayList<>(pivotData.distinctByValues.keySet())
            : getTopValuesInOrder(
                pivotData, Math.min(maxDistinctValues, pivotData.distinctByValues.size()));

    // Add columns
    for (Object byValue : valuesToShow) {
      json.column(new Column(byValue.toString(), response.columnNameTypes().get(valueField)));
    }

    // Build data rows
    List<Object[]> dataRows = buildDataRows(pivotData.pivotData, valuesToShow);
    json.total(dataRows.size()).size(dataRows.size()).datarows(dataRows.toArray(new Object[0][]));
  }

  private List<Object> getTopValuesInOrder(PivotData pivotData, int maxValues) {
    List<Object> topHosts = getTopValuesByScore(pivotData.valueScores, maxValues);
    List<Object> topValues = new ArrayList<>();
    for (Object byValue : pivotData.distinctByValues.keySet()) {
      if (topHosts.contains(byValue)) {
        topValues.add(byValue);
      }
    }
    return topValues;
  }

  private List<Object[]> buildDataRowsWithOther(
      Map<Object, Map<Object, Object>> pivotData, List<Object> topValues) {
    List<Object[]> dataRows = new ArrayList<>();
    for (Map.Entry<Object, Map<Object, Object>> entry : pivotData.entrySet()) {
      Object[] row = new Object[1 + topValues.size() + 1];
      row[0] = entry.getKey();

      Map<Object, Object> byValueMap = entry.getValue();
      for (int i = 0; i < topValues.size(); i++) {
        Object defaultValue = isCountAggregation ? 0 : null;
        row[i + 1] = byValueMap.getOrDefault(topValues.get(i), defaultValue);
      }

      double otherSum =
          byValueMap.entrySet().stream()
              .filter(e -> !topValues.contains(e.getKey()))
              .mapToDouble(e -> convertToDouble(e.getValue()))
              .sum();

      // Set OTHER column value based on aggregation type:
      // Count aggregations: use integer type (0 for no data)
      // Other aggregations: use double type (null for no data)
      if (otherSum != 0.0) {
        row[row.length - 1] = isCountAggregation ? (long) Math.round(otherSum) : otherSum;
      } else {
        row[row.length - 1] = isCountAggregation ? 0 : null;
      }

      dataRows.add(row);
    }
    return dataRows;
  }

  private List<Object[]> buildDataRows(
      Map<Object, Map<Object, Object>> pivotData, List<Object> valuesToShow) {
    List<Object[]> dataRows = new ArrayList<>();
    for (Map.Entry<Object, Map<Object, Object>> entry : pivotData.entrySet()) {
      Object[] row = new Object[1 + valuesToShow.size()];
      row[0] = entry.getKey();

      Map<Object, Object> byValueMap = entry.getValue();
      for (int i = 0; i < valuesToShow.size(); i++) {
        Object defaultValue = isCountAggregation ? 0 : null;
        row[i + 1] = byValueMap.getOrDefault(valuesToShow.get(i), defaultValue);
      }

      dataRows.add(row);
    }
    return dataRows;
  }

  private static class PivotData {
    final Map<Object, Map<Object, Object>> pivotData;
    final Map<Object, Boolean> distinctByValues;
    final Map<Object, Double> valueScores;

    PivotData(
        Map<Object, Map<Object, Object>> pivotData,
        Map<Object, Boolean> distinctByValues,
        Map<Object, Double> valueScores) {
      this.pivotData = pivotData;
      this.distinctByValues = distinctByValues;
      this.valueScores = valueScores;
    }
  }

  private Object[][] fetchDataRows(QueryResult response) {
    Object[][] rows = new Object[response.size()][];
    int i = 0;
    for (Object[] values : response) {
      rows[i++] = values;
    }
    return rows;
  }

  /**
   * Get the top N values by score from the valueScores map.
   *
   * @param valueScores Map of values to their scores
   * @param maxValues Maximum number of values to return
   * @return List of top values sorted by score
   */
  private List<Object> getTopValuesByScore(Map<Object, Double> valueScores, int maxValues) {
    return valueScores.entrySet().stream()
        .sorted(Map.Entry.<Object, Double>comparingByValue().reversed())
        .limit(maxValues)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Convert a value to double for score calculation.
   *
   * @param value Value to convert
   * @return Double value, or 0.0 if conversion fails
   */
  private double convertToDouble(Object value) {
    if (value == null) {
      return 0.0;
    }

    try {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      } else {
        return Double.parseDouble(value.toString());
      }
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }

  /** org.json requires these inner data classes be public (and static) */
  @Builder
  @Getter
  public static class JsonResponse {
    @Singular("column")
    private final List<Column> schema;

    private final Object[][] datarows;

    private long total;
    private long size;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Column {
    private final String name;
    private final String type;
  }
}
