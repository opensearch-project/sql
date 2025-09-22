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
      return resultMap;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * Recursively extracts all attributes from a JSON node, creating path-based field names for
   * nested attributes.
   *
   * @param node The JSON node to extract from
   * @param pathPrefix The current path prefix (empty for root level)
   * @param resultMap The map to store extracted key-value pairs
   */
  private static void extractJsonValueRecursively(
      JsonNode node, String pathPrefix, Map<String, Object> resultMap) {
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        String fullPath = pathPrefix.isEmpty() ? fieldName : pathPrefix + "." + fieldName;
        JsonNode value = field.getValue();

        if (value.isObject()) {
          // Recursively extract nested object
          extractJsonValueRecursively(value, fullPath, resultMap);
        } else {
          // Extract primitive value or array
          Object extractedValue = extractJsonValue(value);
          if (extractedValue instanceof List) {
            resultMap.put(fullPath + ARRAY_SUFFIX, extractedValue);
          } else {
            resultMap.put(fullPath, convertType(extractedValue));
          }
        }
      }
    }
  }

  private static Object extractJsonValue(JsonNode node) {
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
    } else if (node.isArray()) {
      // Extract array as array (not flattened)
      return extractArrayValue(node);
    } else if (node.isObject()) {
      // For objects that are values (not recursively extracted), return as JSON string
      return node.toString();
    } else {
      return node.asText();
    }
  }

  private static Object convertType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof List) {
      return value;
    }
    return value.toString();
  }

  /**
   * Extracts array values, preserving the array structure.
   *
   * @param arrayNode The JSON array node
   * @return List containing the array elements
   */
  private static Object extractArrayValue(JsonNode arrayNode) {
    java.util.List<Object> arrayList = new java.util.ArrayList<>();
    for (JsonNode element : arrayNode) {
      if (element.isObject()) {
        // For objects in arrays, convert to Map instead of JSON string
        Map<String, Object> objectMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = element.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          objectMap.put(field.getKey(), extractJsonValue(field.getValue()));
        }
        arrayList.add(objectMap);
      } else {
        arrayList.add(extractJsonValue(element));
      }
    }
    return arrayList;
  }
}
