package org.opensearch.sql.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

@UtilityClass
public class JsonUtils {
  /**
   * Checks if given JSON string can be parsed as valid JSON.
   *
   * @param jsonExprValue JSON string (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
   * @return true if the string can be parsed as valid JSON, else false.
   */
  public static ExprValue isValidJson(ExprValue jsonExprValue) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      objectMapper.readTree(jsonExprValue.stringValue());
      return ExprValueUtils.LITERAL_TRUE;
    } catch (JsonProcessingException e) {
      return ExprValueUtils.LITERAL_FALSE;
    }
  }
}
