/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_IP;

import org.junit.jupiter.api.Test;

public class OpenSearchExprIpValueTest {

  private OpenSearchExprIpValue ipValue = new OpenSearchExprIpValue("192.168.0.1");

  @Test
  void value() {
    assertEquals("192.168.0.1", ipValue.value());
  }

  @Test
  void type() {
    assertEquals(OPENSEARCH_IP, ipValue.type());
  }

  @Test
  void compare() {
    assertEquals(0, ipValue.compareTo(new OpenSearchExprIpValue("192.168.0.1")));
  }

  @Test
  void equal() {
    assertTrue(ipValue.equal(new OpenSearchExprIpValue("192.168.0.1")));
  }

  @Test
  void testHashCode() {
    assertNotNull(ipValue.hashCode());
  }
}
