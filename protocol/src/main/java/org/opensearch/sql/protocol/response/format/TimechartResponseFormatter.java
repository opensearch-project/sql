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
    // The column order is [timeField, byField, valueField]
    String timeField = columnNames.get(0);
    String byField = columnNames.get(1);
    String valueField = columnNames.get(2);

    // Create a map to store the pivoted data
    // Map<timestamp, Map<byFieldValue, value>>
    Map<Object, Map<Object, Object>> pivotData = new TreeMap<>();

    // Collect all distinct byField values
    Map<Object, Boolean> distinctByValues = new LinkedHashMap<>();

    // Also collect scores for each byValue (sum of all values) for potential OTHER category
    Map<Object, Double> valueScores = new HashMap<>();

    // Collect the data
    for (Object[] row : response) {
      Object timeValue = row[0];
      Object byValue = row[1];
      Object value = row[2];

      distinctByValues.put(byValue, true);

      // Calculate score for each byValue (sum of all values)
      double numericValue = convertToDouble(value);
      valueScores.merge(byValue, numericValue, Double::sum);

      if (!pivotData.containsKey(timeValue)) {
        pivotData.put(timeValue, new HashMap<>());
      }

      pivotData.get(timeValue).put(byValue, value);
    }

    // Build the schema
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    json.column(new Column(timeField, response.columnNameTypes().get(timeField)));

    // Check if we need to create an "OTHER" category (more than maxDistinctValues distinct
    // values)
    // If maxDistinctValues is 0 or useOther is false, we don't need an "OTHER" category
    boolean needsOtherCategory =
        maxDistinctValues > 0 && distinctByValues.size() > maxDistinctValues && useOther;

    if (needsOtherCategory) {
      // Get the top N distinct values based on their scores, but preserve original order
      List<Object> topHosts = getTopValuesByScore(valueScores, maxDistinctValues);
      List<Object> topValues = new ArrayList<>();
      for (Object byValue : distinctByValues.keySet()) {
        if (topHosts.contains(byValue)) {
          topValues.add(byValue);
        }
      }

      // Add columns for top values in original order
      for (Object byValue : topValues) {
        json.column(new Column(byValue.toString(), response.columnNameTypes().get(valueField)));
      }

      // Add OTHER column
      json.column(new Column(OTHER_CATEGORY, response.columnNameTypes().get(valueField)));

      // Build the data rows
      List<Object[]> dataRows = new ArrayList<>();
      for (Map.Entry<Object, Map<Object, Object>> entry : pivotData.entrySet()) {
        Object timeValue = entry.getKey();
        Map<Object, Object> byValueMap = entry.getValue();

        // +1 for time column, +1 for OTHER
        int rowSize = 1 + topValues.size() + 1;
        Object[] row = new Object[rowSize];
        row[0] = timeValue;

        // Fill in values for top categories
        for (int i = 0; i < topValues.size(); i++) {
          row[i + 1] = byValueMap.getOrDefault(topValues.get(i), null);
        }

        // Calculate OTHER value
        double otherSum = 0.0;
        for (Map.Entry<Object, Object> valueEntry : byValueMap.entrySet()) {
          if (!topValues.contains(valueEntry.getKey())) {
            otherSum += convertToDouble(valueEntry.getValue());
          }
        }
        row[rowSize - 1] = otherSum != 0.0 ? otherSum : null;

        dataRows.add(row);
      }

      json.total(dataRows.size()).size(dataRows.size());
      json.datarows(dataRows.toArray(new Object[0][]));
    } else {
      // Handle cases where we don't need OTHER category
      List<Object> valuesToShow;

      if (maxDistinctValues == 0) {
        // Show all distinct values when limit=0
        valuesToShow = new ArrayList<>(distinctByValues.keySet());
      } else {
        // Get top N hosts by score, but preserve original order for display
        List<Object> topHosts =
            getTopValuesByScore(valueScores, Math.min(maxDistinctValues, distinctByValues.size()));
        valuesToShow = new ArrayList<>();
        for (Object byValue : distinctByValues.keySet()) {
          if (topHosts.contains(byValue)) {
            valuesToShow.add(byValue);
          }
        }
      }

      for (Object byValue : valuesToShow) {
        json.column(new Column(byValue.toString(), response.columnNameTypes().get(valueField)));
      }

      // Build the data rows
      List<Object[]> dataRows = new ArrayList<>();
      for (Map.Entry<Object, Map<Object, Object>> entry : pivotData.entrySet()) {
        Object timeValue = entry.getKey();
        Map<Object, Object> byValueMap = entry.getValue();

        Object[] row = new Object[1 + valuesToShow.size()];
        row[0] = timeValue;

        int i = 1;
        for (Object byValue : valuesToShow) {
          row[i++] = byValueMap.getOrDefault(byValue, null);
        }

        dataRows.add(row);
      }

      json.total(dataRows.size()).size(dataRows.size());
      json.datarows(dataRows.toArray(new Object[0][]));
    }

    return json.build();
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
