/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class DebugUtilsTest {

  @Test
  public void testDebugThrowsRuntimeException() {
    assertThrows(RuntimeException.class, () -> DebugUtils.debug("test", "test message"));
  }

  @Test
  public void testDebugWithoutMessageThrowsRuntimeException() {
    assertThrows(RuntimeException.class, () -> DebugUtils.debug("test"));
  }
}
