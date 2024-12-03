/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchIpType;

public class OpenSearchExprIpValueTest {

  private final String ipString = "192.168.0.1";
  private final OpenSearchExprIpValue ipValue = new OpenSearchExprIpValue(ipString);

  @Test
  void testValue() {
    assertEquals(ipString, ipValue.value());
  }

  @Test
  void testType() {
    assertEquals(OpenSearchIpType.of(), ipValue.type());
  }

  @Test
  void testCompare() {
    assertEquals(0, ipValue.compareTo(new OpenSearchExprIpValue(ipString)));
    assertEquals(ipValue, new OpenSearchExprIpValue(ipString));
  }

  @Test
  void testEqual() {
    assertTrue(ipValue.equal(new OpenSearchExprIpValue(ipString)));
  }

  @Test
  void testHashCode() {
    assertNotNull(ipValue.hashCode());
  }
}
