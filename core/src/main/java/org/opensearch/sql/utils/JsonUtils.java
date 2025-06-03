/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;

@UtilityClass
public class JsonUtils {
  /**
   * Checks if given JSON string can be parsed as valid JSON.
   *
   * @param jsonExprValue JSON string (e.g. "{\"hello\": \"world\"}").
   * @return true if the string can be parsed as valid JSON, else false (including null or missing).
   */
  public static ExprValue isValidJson(ExprValue jsonExprValue) {
    ObjectMapper objectMapper = new ObjectMapper();

    if (jsonExprValue.isNull() || jsonExprValue.isMissing()) {
      return ExprValueUtils.LITERAL_FALSE;
    }

    try {
      objectMapper.readTree(jsonExprValue.stringValue());
      return LITERAL_TRUE;
    } catch (JsonProcessingException e) {
      return LITERAL_FALSE;
    }
  }

  /**
   * Converts a JSON encoded string to a {@link ExprValue}. Expression type will be UNDEFINED.
   *
   * @param json JSON string (e.g. "{\"hello\": \"world\"}").
   * @return ExprValue returns an expression that best represents the provided JSON-encoded string.
   *     <ol>
   *       <li>{@link ExprTupleValue} if the JSON is an object
   *       <li>{@link ExprCollectionValue} if the JSON is an array
   *       <li>{@link ExprDoubleValue} if the JSON is a floating-point number scalar
   *       <li>{@link ExprIntegerValue} if the JSON is an integral number scalar
   *       <li>{@link ExprStringValue} if the JSON is a string scalar
   *       <li>{@link ExprBooleanValue} if the JSON is a boolean scalar
   *       <li>{@link ExprNullValue} if the JSON is null, empty, or invalid
   *     </ol>
   */
  public static ExprValue castJson(ExprValue json) {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode;
    try {
      jsonNode = objectMapper.readTree(json.stringValue());
    } catch (JsonProcessingException e) {
      final String errorFormat = "JSON string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, json, e.getMessage()), e);
    }

    return processJsonNode(jsonNode);
  }

  private static ExprValue processJsonNode(JsonNode jsonNode) {
    switch (jsonNode.getNodeType()) {
      case ARRAY:
        List<ExprValue> elements = new LinkedList<>();
        for (var iter = jsonNode.iterator(); iter.hasNext(); ) {
          jsonNode = iter.next();
          elements.add(processJsonNode(jsonNode));
        }
        return new ExprCollectionValue(elements);
      case OBJECT:
        Map<String, ExprValue> values = new LinkedHashMap<>();
        for (var iter = jsonNode.fields(); iter.hasNext(); ) {
          Map.Entry<String, JsonNode> entry = iter.next();
          values.put(entry.getKey(), processJsonNode(entry.getValue()));
        }
        return ExprTupleValue.fromExprValueMap(values);
      case STRING:
        return new ExprStringValue(jsonNode.asText());
      case NUMBER:
        if (jsonNode.isFloatingPointNumber()) {
          return new ExprDoubleValue(jsonNode.asDouble());
        }
        return new ExprIntegerValue(jsonNode.asLong());
      case BOOLEAN:
        return jsonNode.asBoolean() ? LITERAL_TRUE : LITERAL_FALSE;
      default:
        // in all other cases, return null
        return LITERAL_NULL;
    }
  }
}
