/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** Query-execution backends, each paired with the {@link Capability} set it does not support. */
public enum Backend {
  OPENSEARCH,
  ANALYTICS_ENGINE;

  /**
   * {@code OPENSEARCH} lacks only the capabilities listed here; every other capability in the
   * registry is an analytics-engine gap, so {@code ANALYTICS_ENGINE} takes the complement.
   */
  private static final Map<Backend, Set<Capability>> UNSUPPORTED =
      Map.of(
          OPENSEARCH,
          EnumSet.of(Capability.SET_OPERATION),
          ANALYTICS_ENGINE,
          EnumSet.complementOf(EnumSet.of(Capability.SET_OPERATION)));

  /** Whether this backend can run a test declaring {@code capability}. */
  public boolean supports(Capability capability) {
    return !UNSUPPORTED.get(this).contains(capability);
  }
}
