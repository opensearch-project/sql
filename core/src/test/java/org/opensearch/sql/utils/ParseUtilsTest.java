/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

public class ParseUtilsTest {
  @Test
  void test_parse_group_and_value() {
    ExprValue exprValue = new ExprStringValue(
        "130.246.123.197 - - [2018-07-22T03:26:21.326Z] \"GET /beats/metricbeat_1 HTTP/1.1\" "
            + "200 6850 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 "
            + "Firefox/6.0a1\"");
    String rawPattern =
        "(?<ip>(\\d{1,3}\\.){3}\\d{1,3}) - - \\[(?<date>\\d{4}\\-[01]\\d\\-[0-3]\\dT[0-2]\\d:"
            + "[0-5]\\d:[0-5]\\d\\.\\d+([+-][0-2]\\d:[0-5]\\d|Z))\\] \\\"(?<request>[^\\\"]+)\\\" "
            + "(?<status>\\d+) (?<bytes>\\d+) \\\"-\\\" \\\"(?<userAgent>[^\\\"]+)\\\"";
    Pattern pattern = Pattern.compile(rawPattern);
    Map<String, String> expected =
        ImmutableMap.of("ip", "130.246.123.197", "date", "2018-07-22T03:26:21.326Z", "request",
            "GET /beats/metricbeat_1 HTTP/1.1", "status", "200", "bytes", "6850", "userAgent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1");
    List<String> identifiers = new ArrayList<>(expected.keySet());
    assertEquals(identifiers, ParseUtils.getNamedGroupCandidates(rawPattern));
    identifiers.forEach(identifier -> assertEquals(new ExprStringValue(expected.get(identifier)),
        ParseUtils.parseValue(exprValue, pattern, identifier)));
  }

  @Test
  void test_null_missing_non_match() {
    ExprValue nullValue = ExprValueUtils.nullValue();
    ExprValue missingValue = ExprValueUtils.missingValue();
    Pattern pattern = Pattern.compile("(<group>\\d+)");
    assertEquals(nullValue, ParseUtils.parseValue(nullValue, pattern, "group"));
    assertEquals(nullValue, ParseUtils.parseValue(missingValue, pattern, "group"));
    assertEquals(new ExprStringValue(""),
        ParseUtils.parseValue(new ExprStringValue("non match"), pattern, "group"));
  }
}
