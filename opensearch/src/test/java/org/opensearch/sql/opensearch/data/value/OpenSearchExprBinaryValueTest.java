/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchBinaryType;

public class OpenSearchExprBinaryValueTest {

  @Test
  public void compare() {
    assertEquals(
        0,
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")
            .compare(new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void equal() {
    OpenSearchExprBinaryValue value = new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertTrue(value.equal(new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void value() {
    OpenSearchExprBinaryValue value = new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals("U29tZSBiaW5hcnkgYmxvYg==", value.value());
  }

  @Test
  public void type() {
    OpenSearchExprBinaryValue value = new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals(OpenSearchBinaryType.of(), value.type());
  }
}
