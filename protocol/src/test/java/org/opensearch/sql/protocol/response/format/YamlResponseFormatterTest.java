/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.utils.YamlFormatter;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class YamlResponseFormatterTest {

  private final YamlResponseFormatter<Object> formatter =
      new YamlResponseFormatter<>() {
        @Override
        protected Object buildYamlObject(Object response) {
          // Pass-through for testing: return the response directly
          return response;
        }
      };

  @Test
  void content_type_matches_yaml() {
    assertEquals(YamlResponseFormatter.CONTENT_TYPE, formatter.contentType());
  }

  @Test
  void formats_response_via_yaml_formatter() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("b", 2);
    payload.put("a", "1");

    String expected = YamlFormatter.formatToYaml(payload);
    String actual = formatter.format(payload);

    assertEquals(expected, actual);
  }

  @Test
  void formats_throwable_via_yaml_formatter() {
    Exception e = new Exception("boom", new RuntimeException("root-cause"));

    String expected = YamlFormatter.formatToYaml(e);
    String actual = formatter.format(e);

    assertEquals(expected, actual);
  }
}
