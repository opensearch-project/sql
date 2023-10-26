/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprArrayValue;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;

/** Default implementation of SparkSqlFunctionResponseHandle. */
public class DefaultSparkSqlFunctionResponseHandle implements SparkSqlFunctionResponseHandle {
  public static final String DATA = "data";
  public static final String APPLICATION_ID = "applicationId";
  public static final String SCHEMA = "schema";
  public static final String RESULT = "result";
  public static final String COLUMN_NAME = "column_name";
  public static final String DATA_TYPE = "data_type";
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
    JSONObject items = responseObject.getJSONObject(DATA);
    logger.info("Spark Application ID: " + items.getString(APPLICATION_ID));
    columnList = getColumnList(items.getJSONArray(SCHEMA));
    for (int i = 0; i < items.getJSONArray(RESULT).length(); i++) {
      JSONObject row =
          new JSONObject(items.getJSONArray(RESULT).get(i).toString().replace("'", "\""));
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
      if (type == ExprCoreType.BOOLEAN) {
        linkedHashMap.put(column.getName(), ExprBooleanValue.of(row.getBoolean(column.getName())));
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
        linkedHashMap.put(column.getName(), new ExprStringValue(jsonString(row, column.getName())));
      } else if (type == ExprCoreType.STRUCT) {
        linkedHashMap.put(column.getName(), exprStructBuilder(row, column.getName()));
      } else if (type == ExprCoreType.ARRAY) {
        linkedHashMap.put(column.getName(), exprArrayBuilder(row, column.getName()));
      } else {
        throw new RuntimeException("Result contains invalid data type");
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
              column.get(COLUMN_NAME).toString(),
              column.get(COLUMN_NAME).toString(),
              getDataType(column.get(DATA_TYPE).toString())));
    }
    return columnList;
  }

  private ExprCoreType getDataType(String sparkDataType) {
    switch (sparkDataType) {
      case "struct":
        return ExprCoreType.STRUCT;
      case "array":
        return ExprCoreType.ARRAY;
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
      case "varchar":
      case "char":
        return ExprCoreType.STRING;
      default:
        return ExprCoreType.UNKNOWN;
    }
  }

  private static String jsonString(JSONObject jsonObject, String key) {
    return jsonObject.has(key) ? jsonObject.getString(key) : "";
  }

  private static ExprArrayValue exprArrayBuilder(JSONObject jsonObject, String key) {
    return (ExprArrayValue) new JsonVisitorImpl().visit(jsonObject.get(key));
  }

  private static ExprTupleValue exprStructBuilder(JSONObject jsonObject, String key) {
    return (ExprTupleValue) new JsonVisitorImpl().visit(jsonObject.get(key));
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

  /** visitor implementation for traversing the json object */
  public static class JsonVisitorImpl implements JsonVisitor<ExprValue> {
    public ExprValue visit(Object obj) {
      if (obj instanceof JSONObject) {
        return visitObject((JSONObject) obj);
      } else if (obj instanceof JSONArray) {
        return visitArray((JSONArray) obj);
      } else {
        return visitPrimitive(obj);
      }
    }

    @Override
    public ExprValue visitObject(JSONObject jsonObject) {
      return fromObjectValue(
          jsonObject.keySet().stream()
              .collect(Collectors.toMap(key -> key, key -> visit(jsonObject.get(key)))));
    }

    @Override
    public ExprValue visitArray(JSONArray jsonArray) {
      return new ExprArrayValue(
          stream(spliteratorUnknownSize(jsonArray.iterator(), Spliterator.ORDERED), false)
              .map(this::visit)
              .collect(Collectors.toList()));
    }

    @Override
    public ExprValue visitPrimitive(Object primitive) {
      return fromObjectValue(primitive);
    }
  }
}
