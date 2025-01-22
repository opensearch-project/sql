package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
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
   * @return true if the string can be parsed as valid JSON, else false.
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

  /** Converts a JSON encoded string to an Expression object. */
  public static ExprValue castJson(ExprValue json) {
    JsonNode jsonNode = jsonToNode(json);
    return processJsonNode(jsonNode);
  }

  public static ExprValue extractJson(ExprValue json, ExprValue path) {
    String jsonString = json.stringValue();
    String jsonPath = path.stringValue();

    if (jsonString.equals("")) {
      return LITERAL_NULL;
    }

    try {
      Configuration config = Configuration.builder()
              .options(Option.AS_PATH_LIST)
              .build();
      List<String> resultPaths = JsonPath.using(config).parse(jsonString).read(jsonPath);

      List<ExprValue> elements = new LinkedList<>();
      for (String resultPath : resultPaths) {
        Object result = JsonPath.parse(jsonString).read(resultPath);
        elements.add(ExprValueUtils.fromObjectValue(result));
      }

      if (elements.size() == 1) {
        return elements.get(0);
      } else {
        return new ExprCollectionValue(elements);
      }
    } catch (PathNotFoundException e) {
      return LITERAL_NULL;
    } catch (InvalidJsonException e) {
      final String errorFormat = "JSON string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, json, e.getMessage()), e);
    }
  }

  private static JsonNode jsonToNode(ExprValue json) {
    return jsonStringToNode(json.stringValue());
  }

  private static JsonNode jsonStringToNode(String jsonString) {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode;
    try {
      jsonNode = objectMapper.readTree(jsonString);
    } catch (JsonProcessingException e) {
      final String errorFormat = "JSON string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, jsonString, e.getMessage()), e);
    }
    return jsonNode;
  }

  private static ExprValue processJsonNode(JsonNode jsonNode) {
    if (jsonNode.isFloatingPointNumber()) {
      return new ExprDoubleValue(jsonNode.asDouble());
    }
    if (jsonNode.isIntegralNumber()) {
      return new ExprIntegerValue(jsonNode.asLong());
    }
    if (jsonNode.isBoolean()) {
      return jsonNode.asBoolean() ? LITERAL_TRUE : LITERAL_FALSE;
    }
    if (jsonNode.isTextual()) {
      return new ExprStringValue(jsonNode.asText());
    }
    if (jsonNode.isArray()) {
      List<ExprValue> elements = new LinkedList<>();
      for (var iter = jsonNode.iterator(); iter.hasNext(); ) {
        jsonNode = iter.next();
        elements.add(processJsonNode(jsonNode));
      }
      return new ExprCollectionValue(elements);
    }
    if (jsonNode.isObject()) {
      Map<String, ExprValue> values = new LinkedHashMap<>();
      for (var iter = jsonNode.fields(); iter.hasNext(); ) {
        Map.Entry<String, JsonNode> entry = iter.next();
        values.put(entry.getKey(), processJsonNode(entry.getValue()));
      }
      return ExprTupleValue.fromExprValueMap(values);
    }

    // in all other cases, return null
    return LITERAL_NULL;
  }
}
