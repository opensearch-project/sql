/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.MAP_STRING_ANY_FORCE_NULLABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class JsonExtractAllFunctionImpl extends ImplementorUDF {
  private static final String ARRAY_SUFFIX = "{}";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonExtractAllFunctionImpl() {
    super(new JsonExtractAllImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return MAP_STRING_ANY_FORCE_NULLABLE;
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

    try {
      JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonStr);
      if (!jsonNode.isObject()) {
        return null;
      }

      Map<String, Object> resultMap = new HashMap<>();
      extractJsonValueRecursively(jsonNode, "", resultMap);
      return convertArraysToStrings(resultMap);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * Recursively extracts all attributes from a JSON node using simplified path tracking logic.
   *
   * @param node The JSON node to extract from
   * @param currentPath The current path (empty for root level)
   * @param resultMap The map to store extracted key-value pairs
   */
  private static void extractJsonValueRecursively(
      JsonNode node, String currentPath, Map<String, Object> resultMap) {
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        String newPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
        JsonNode value = field.getValue();

        visitValue(value, newPath, resultMap);
      }
    } else if (node.isArray()) {
      String arrayPath = currentPath + ARRAY_SUFFIX;
      for (JsonNode element : node) {
        if (element.isObject() || element.isArray()) {
          extractJsonValueRecursively(element, arrayPath, resultMap);
        } else {
          visitValue(element, arrayPath, resultMap);
        }
      }
    }
  }

  private static void visitValue(JsonNode node, String path, Map<String, Object> resultMap) {
    if (node.isObject() || node.isArray()) {
      extractJsonValueRecursively(node, path, resultMap);
    } else {
      Object value = extractPrimitiveValue(node);
      addValueToResult(path, value, resultMap);
    }
  }

  private static Object extractPrimitiveValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isDouble()) {
      return node.doubleValue();
    } else if (node.isTextual()) {
      return node.textValue();
    } else {
      return node.asText();
    }
  }

  @SuppressWarnings("unchecked")
  private static void addValueToResult(String path, Object value, Map<String, Object> resultMap) {
    if (resultMap.containsKey(path)) {
      Object existing = resultMap.get(path);
      if (existing instanceof java.util.List) {
        // Already an array, add to it
        ((java.util.List<Object>) existing).add(convertToString(value));
      } else {
        // Convert to array and add both values
        java.util.List<Object> arrayList = new java.util.ArrayList<>();
        arrayList.add(existing);
        arrayList.add(convertToString(value));
        resultMap.put(path, arrayList);
      }
    } else {
      // First value at this path
      resultMap.put(path, convertToString(value));
    }
  }

  // Currently, we convert all primitive types to String
  private static String convertToString(Object obj) {
    return obj != null ? obj.toString() : null;
  }

  /**
   * Converts any arrays in the result map to string representations. This ensures all values in the
   * result are either String or null.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> convertArraysToStrings(Map<String, Object> resultMap) {
    Map<String, Object> convertedMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof java.util.List) {
        convertedMap.put(entry.getKey(), convertArrayToString((java.util.List<Object>) value));
      } else {
        convertedMap.put(entry.getKey(), value);
      }
    }
    return convertedMap;
  }

  /** Converts a list to its JSON array string representation. */
  private static String convertArrayToString(java.util.List<Object> list) {
    if (list == null || list.isEmpty()) {
      return "[]";
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(list);
    } catch (JsonProcessingException e) {
      // Fallback to empty array if serialization fails
      return "[]";
    }
  }
}
