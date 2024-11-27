/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import inet.ipaddr.AddressStringException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;

public class ExprIpValueTest {

  private static final String ipv4String = "1.2.3.4";
  private static final String ipv6String = "2001:db7::ff00:42:8329";
  private static final String ipInvalidString = "INVALID";

  private static final ExprValue exprIpv4Value = ExprValueUtils.ipValue(ipv4String);
  private static final ExprValue exprIpv6Value = ExprValueUtils.ipValue(ipv6String);

  private static final List<String> ipv4LesserStrings =
      List.of("1.2.3.3", "01.2.3.3", "::ffff:1.2.3.3", "::ffff:102:303");
  private static final List<String> ipv4EqualStrings =
      List.of("1.2.3.4", "01.2.3.4", "::ffff:1.2.3.4", "::ffff:102:304");
  private static final List<String> ipv4GreaterStrings =
      List.of("1.2.3.5", "01.2.3.5", "::ffff:1.2.3.5", "::ffff:102:305");

  private static final List<String> ipv6LesserStrings =
      List.of(
          "2001:db7::ff00:42:8328",
          "2001:0db7::ff00:0042:8328",
          "2001:DB7::FF00:42:8328",
          "2001:0db7:0000:0000:0000:ff00:0042:8328");
  private static final List<String> ipv6EqualStrings =
      List.of(
          "2001:db7::ff00:42:8329",
          "2001:0db7::ff00:0042:8329",
          "2001:DB7::FF00:42:8329",
          "2001:0db7:0000:0000:0000:ff00:0042:8329");
  private static final List<String> ipv6GreaterStrings =
      List.of(
          "2001:db7::ff00:42:8330",
          "2001:0db7::ff00:0042:8330",
          "2001:DB7::FF00:42:8330",
          "2001:0db7:0000:0000:0000:ff00:0042:8330");

  @Test
  public void testInvalid() {
    Exception ex =
        assertThrows(SemanticCheckException.class, () -> ExprValueUtils.ipValue(ipInvalidString));
    assertTrue(
        ex.getMessage()
            .matches(
                String.format("IP address '%s' is not valid. Error details: .*", ipInvalidString)));
  }

  @Test
  public void testValue() throws AddressStringException {
    ipv4EqualStrings.forEach((s) -> assertEquals(ipv4String, ExprValueUtils.ipValue(s).value()));
    ipv6EqualStrings.forEach((s) -> assertEquals(ipv6String, ExprValueUtils.ipValue(s).value()));
  }

  @Test
  public void testType() {
    assertEquals(ExprCoreType.IP, exprIpv4Value.type());
    assertEquals(ExprCoreType.IP, exprIpv6Value.type());
  }

  @Test
  public void testCompare() {
    ipv4LesserStrings.forEach(
        (s) -> assertTrue(exprIpv4Value.compareTo(ExprValueUtils.ipValue(s)) > 0));
    ipv4EqualStrings.forEach(
        (s) -> assertEquals(0, exprIpv4Value.compareTo(ExprValueUtils.ipValue(s))));
    ipv4GreaterStrings.forEach(
        (s) -> assertTrue(exprIpv4Value.compareTo(ExprValueUtils.ipValue(s)) < 0));
    ipv6LesserStrings.forEach(
        (s) -> assertTrue(exprIpv6Value.compareTo(ExprValueUtils.ipValue(s)) > 0));
    ipv6EqualStrings.forEach(
        (s) -> assertEquals(0, exprIpv6Value.compareTo(ExprValueUtils.ipValue(s))));
    ipv6GreaterStrings.forEach(
        (s) -> assertTrue(exprIpv6Value.compareTo(ExprValueUtils.ipValue(s)) < 0));
  }

  @Test
  public void testEquals() {
    ipv4EqualStrings.forEach((s) -> assertEquals(exprIpv4Value, ExprValueUtils.ipValue(s)));
    ipv6EqualStrings.forEach((s) -> assertEquals(exprIpv6Value, ExprValueUtils.ipValue(s)));
  }

  @Test
  public void testToString() {
    ipv4EqualStrings.forEach(
        (s) ->
            assertEquals(String.format("IP %s", ipv4String), ExprValueUtils.ipValue(s).toString()));
    ipv6EqualStrings.forEach(
        (s) ->
            assertEquals(String.format("IP %s", ipv6String), ExprValueUtils.ipValue(s).toString()));
  }
}
