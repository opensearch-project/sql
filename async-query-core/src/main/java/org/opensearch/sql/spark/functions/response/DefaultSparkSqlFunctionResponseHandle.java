/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.data.type.SparkDataType;
import org.opensearch.sql.spark.data.value.SparkExprValue;

/** Default implementation of SparkSqlFunctionResponseHandle. */
public class DefaultSparkSqlFunctionResponseHandle implements SparkSqlFunctionResponseHandle {
  private Iterator<ExprValue> responseIterator;
  private ExecutionEngine.Schema schema;
  private static final Logger logger =
      LogManager.getLogger(DefaultSparkSqlFunctionResponseHandle.class);

  /**
   * Constructor.
   *
   * @param responseObject Spark responseObject.
   */
  public DefaultSparkSqlFunctionResponseHandle(JSONObject responseObject) {
    constructIteratorAndSchema(responseObject);
  }

  private void constructIteratorAndSchema(JSONObject responseObject) {
    List<ExprValue> result = new ArrayList<>();
    List<ExecutionEngine.Schema.Column> columnList;
    JSONObject items = responseObject.getJSONObject("data");
    columnList = getColumnList(items.getJSONArray("schema"));
    for (int i = 0; i < items.getJSONArray("result").length(); i++) {
      JSONObject row =
          new JSONObject(items.getJSONArray("result").get(i).toString().replace("'", "\""));
      LinkedHashMap<String, ExprValue> linkedHashMap = extractRow(row, columnList);
      result.add(new ExprTupleValue(linkedHashMap));
    }
    this.schema = new ExecutionEngine.Schema(columnList);
    this.responseIterator = result.iterator();
  }

  private static LinkedHashMap<String, ExprValue> extractRow(
      JSONObject row, List<ExecutionEngine.Schema.Column> columnList) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
    for (ExecutionEngine.Schema.Column column : columnList) {
      ExprType type = column.getExprType();
      if (!row.has(column.getName())) {
        linkedHashMap.put(column.getName(), ExprNullValue.of());
      } else {
        if (type == ExprCoreType.BOOLEAN) {
          linkedHashMap.put(
              column.getName(), ExprBooleanValue.of(row.getBoolean(column.getName())));
        } else if (type == ExprCoreType.LONG) {
          linkedHashMap.put(column.getName(), new ExprLongValue(row.getLong(column.getName())));
        } else if (type == ExprCoreType.INTEGER) {
          linkedHashMap.put(column.getName(), new ExprIntegerValue(row.getInt(column.getName())));
        } else if (type == ExprCoreType.SHORT) {
          linkedHashMap.put(column.getName(), new ExprShortValue(row.getInt(column.getName())));
        } else if (type == ExprCoreType.BYTE) {
          linkedHashMap.put(column.getName(), new ExprByteValue(row.getInt(column.getName())));
        } else if (type == ExprCoreType.DOUBLE) {
          linkedHashMap.put(column.getName(), new ExprDoubleValue(row.getDouble(column.getName())));
        } else if (type == ExprCoreType.FLOAT) {
          linkedHashMap.put(column.getName(), new ExprFloatValue(row.getFloat(column.getName())));
        } else if (type == ExprCoreType.DATE) {
          // TODO :: correct this to ExprTimestampValue
          linkedHashMap.put(column.getName(), new ExprStringValue(row.getString(column.getName())));
        } else if (type == ExprCoreType.TIMESTAMP) {
          linkedHashMap.put(
              column.getName(), new ExprTimestampValue(row.getString(column.getName())));
        } else if (type == ExprCoreType.STRING) {
          linkedHashMap.put(column.getName(), new ExprStringValue(row.getString(column.getName())));
        } else {
          // SparkDataType
          Object jsonValue = row.get(column.getName());
          Object value = jsonValue;
          if (jsonValue instanceof JSONObject) {
            value = ((JSONObject) jsonValue).toMap();
          } else if (jsonValue instanceof JSONArray) {
            value = ((JSONArray) jsonValue).toList();
          }
          linkedHashMap.put(column.getName(), new SparkExprValue((SparkDataType) type, value));
        }
      }
    }

    return linkedHashMap;
  }

  private List<ExecutionEngine.Schema.Column> getColumnList(JSONArray schema) {
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = new JSONObject(schema.get(i).toString().replace("'", "\""));
      columnList.add(
          new ExecutionEngine.Schema.Column(
              column.get("column_name").toString(),
              column.get("column_name").toString(),
              getDataType(column.get("data_type").toString())));
    }
    return columnList;
  }

  private ExprType getDataType(String sparkType) {
    switch (sparkType) {
      case "boolean":
        return ExprCoreType.BOOLEAN;
      case "long":
        return ExprCoreType.LONG;
      case "integer":
        return ExprCoreType.INTEGER;
      case "short":
        return ExprCoreType.SHORT;
      case "byte":
        return ExprCoreType.BYTE;
      case "double":
        return ExprCoreType.DOUBLE;
      case "float":
        return ExprCoreType.FLOAT;
      case "timestamp":
        return ExprCoreType.DATE;
      case "date":
        return ExprCoreType.TIMESTAMP;
      case "string":
        return ExprCoreType.STRING;
      default:
        return new SparkDataType(sparkType);
    }
  }

  @Override
  public boolean hasNext() {
    return responseIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return responseIterator.next();
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return schema;
  }
}
