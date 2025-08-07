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
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * JSON response formatter specifically for timechart command.
 * It transforms the data from [timestamp, field, value] format to a pivot table format:
 * [timestamp, field1_value, field2_value, ...].
 */
public class TimechartResponseFormatter extends JsonResponseFormatter<QueryResult> {

  public TimechartResponseFormatter(Style style) {
    super(style);
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
    
    // Collect the data
    for (Object[] row : response) {
      Object timeValue = row[0];
      Object byValue = row[1];
      Object value = row[2];
      
      distinctByValues.put(byValue, true);
      
      if (!pivotData.containsKey(timeValue)) {
        pivotData.put(timeValue, new HashMap<>());
      }
      
      pivotData.get(timeValue).put(byValue, value);
    }
    
    // Build the schema
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();
    json.column(new Column(timeField, response.columnNameTypes().get(timeField)));
    
    for (Object byValue : distinctByValues.keySet()) {
      json.column(new Column(byValue.toString(), response.columnNameTypes().get(valueField)));
    }
    
    // Build the data rows
    List<Object[]> dataRows = new ArrayList<>();
    for (Map.Entry<Object, Map<Object, Object>> entry : pivotData.entrySet()) {
      Object timeValue = entry.getKey();
      Map<Object, Object> byValueMap = entry.getValue();
      
      Object[] row = new Object[1 + distinctByValues.size()];
      row[0] = timeValue;
      
      int i = 1;
      for (Object byValue : distinctByValues.keySet()) {
        row[i++] = byValueMap.getOrDefault(byValue, null);
      }
      
      dataRows.add(row);
    }
    
    json.total(dataRows.size()).size(dataRows.size());
    json.datarows(dataRows.toArray(new Object[0][]));
    
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
