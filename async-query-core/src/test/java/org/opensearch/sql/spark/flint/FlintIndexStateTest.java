/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.spark.flint.FlintIndexState.UNKNOWN;

import org.junit.jupiter.api.Test;

class FlintIndexStateTest {
  @Test
  public void unknownState() {
    assertEquals(UNKNOWN, FlintIndexState.fromString("noSupported"));
  }
}
