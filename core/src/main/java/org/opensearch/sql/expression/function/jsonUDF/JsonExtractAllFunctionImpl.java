/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * UDF which extract all the fields from JSON to a MAP. Items are collected from input JSON and
 * stored with the key of their path in the JSON. This UDF is designed to be used for `spath`
 * command without path param. See {@ref JsonExtractAllFunctionImplTest} for the detailed spec.
 */
public class JsonExtractAllFunctionImpl extends ImplementorUDF {
  private static final String ARRAY_SUFFIX = "{}";
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  public JsonExtractAllFunctionImpl() {
    super(new JsonExtractAllImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.ANY),
            true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.STRING));
  }

  public static class JsonExtractAllImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonExtractAllFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    if (args.length < 1) {
      return null;
    }

    String jsonStr = (String) args[0];
    if (jsonStr == null || jsonStr.trim().isEmpty()) {
      return null;
    }

    return parseJson(jsonStr);
  }

  private static Map<String, Object> parseJson(String jsonStr) {
    Map<String, Object> resultMap = new HashMap<>();
    Stack<String> pathStack = new Stack<>();

    try (JsonParser parser = JSON_FACTORY.createParser(jsonStr)) {
      JsonToken token;

      while ((token = parser.nextToken()) != null) {
        switch (token) {
          case START_OBJECT:
            break;

          case END_OBJECT:
            if (!pathStack.isEmpty() && !isInArray(pathStack)) {
              pathStack.pop();
            }
            break;

          case START_ARRAY:
            pathStack.push(ARRAY_SUFFIX);
            break;

          case END_ARRAY:
            pathStack.pop();
            if (!pathStack.isEmpty() && !isInArray(pathStack)) {
              pathStack.pop();
            }
            break;

          case FIELD_NAME:
            String fieldName = parser.currentName();
            pathStack.push(fieldName);
            break;

          case VALUE_STRING:
          case VALUE_NUMBER_INT:
          case VALUE_NUMBER_FLOAT:
          case VALUE_TRUE:
          case VALUE_FALSE:
          case VALUE_NULL:
            if (pathStack.isEmpty()) {
              // ignore top level value
              return null;
            }

            appendValue(resultMap, buildPath(pathStack), extractValue(parser, token));

            if (!isInArray(pathStack)) {
              pathStack.pop();
            }
            break;
          default:
            // Skip other tokens
            break;
        }
      }
    } catch (IOException e) {
      // ignore exception, and current result will be returned
    }
    return resultMap;
  }

  @SuppressWarnings("unchecked")
  private static void appendValue(Map<String, Object> resultMap, String path, Object value) {
    Object existingValue = resultMap.get(path);
    if (existingValue == null) {
      resultMap.put(path, value);
    } else if (existingValue instanceof List) {
      ((List<Object>) existingValue).add(value);
    } else {
      resultMap.put(path, list(existingValue, value));
    }
  }

  private static List<Object> list(Object... args) {
    List<Object> result = new LinkedList<>();
    for (Object arg : args) {
      result.add(arg);
    }
    return result;
  }

  private static boolean isInArray(List<String> path) {
    return path.size() >= 1 && path.getLast().equals(ARRAY_SUFFIX);
  }

  private static Object extractValue(JsonParser parser, JsonToken token) throws IOException {
    switch (token) {
      case VALUE_STRING:
        return parser.getValueAsString();
      case VALUE_NUMBER_INT:
        return getIntValue(parser);
      case VALUE_NUMBER_FLOAT:
        return parser.getDoubleValue();
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      case VALUE_NULL:
        return null;
      default:
        return parser.getValueAsString();
    }
  }

  private static Object getIntValue(JsonParser parser) throws IOException {
    if (parser.getNumberType() == JsonParser.NumberType.INT) {
      return parser.getIntValue();
    } else if (parser.getNumberType() == JsonParser.NumberType.LONG) {
      return parser.getLongValue();
    } else {
      // store as double in case of BIG_INTEGER (exceed LONG precision)
      return parser.getBigIntegerValue().doubleValue();
    }
  }

  private static String buildPath(Collection<String> pathStack) {
    StringBuilder builder = new StringBuilder();
    for (String path : pathStack) {
      if (ARRAY_SUFFIX.equals(path)) {
        builder.append(ARRAY_SUFFIX);
      } else if (!path.isEmpty()) {
        if (!builder.isEmpty()) {
          builder.append(".");
        }
        builder.append(path);
      }
    }
    return builder.toString();
  }
}
