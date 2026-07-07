/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import org.junit.Assume;
import org.opensearch.sql.legacy.TestUtils;

/** Skips integration tests that require a {@link Capability} the active backend lacks. */
public final class BackendCapabilities {

  public static void requireCapability(Capability capability) {
    requireCapability(capability, null);
  }

  public static void requireCapability(Capability capability, String note) {
    // Today analytics-engine supports none of the defined capabilities, so all are skipped on it.
    Assume.assumeTrue(skipMessage(capability, note), !TestUtils.AnalyticsIndexConfig.isEnabled());
  }

  private static String skipMessage(Capability capability, String note) {
    String base = capability.name() + ": " + capability.reason();
    return (note == null || note.isBlank()) ? base : base + " — " + note;
  }

  private BackendCapabilities() {}
}
