/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.format;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class ErrorFormatterTest {

  // https://www.javadoc.io/doc/com.google.code.gson/gson/2.7/com/google/gson/GsonBuilder.html#disableHtmlEscaping--
  @Test
  void htmlEscaping_should_disabled() {
    assertEquals(
        format("{%n" + "  \"request\": \"index=test\"%n" + "}"),
        ErrorFormatter.prettyJsonify(ImmutableMap.of("request", "index=test")));
    assertEquals(
        "{\"request\":\"index=test\"}",
        ErrorFormatter.compactJsonify(ImmutableMap.of("request", "index=test")));
  }
}
