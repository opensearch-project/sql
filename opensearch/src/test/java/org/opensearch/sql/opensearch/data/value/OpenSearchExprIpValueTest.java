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

  private OpenSearchExprIpValue ipValue = new OpenSearchExprIpValue("192.168.0.1");

  @Test
  void value() {
    assertEquals("192.168.0.1", ipValue.value());
  }

  @Test
  void type() {
    assertEquals(OpenSearchIpType.of(), ipValue.type());
  }

  @Test
  void compare() {
    assertEquals(0, ipValue.compareTo(new OpenSearchExprIpValue("192.168.0.1")));
    assertEquals(ipValue, new OpenSearchExprIpValue("192.168.0.1"));
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
