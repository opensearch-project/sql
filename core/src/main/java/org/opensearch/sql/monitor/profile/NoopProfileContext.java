/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Disabled profiling context. */
public final class NoopProfileContext implements ProfileContext {

  public static final NoopProfileContext INSTANCE = new NoopProfileContext();

  private NoopProfileContext() {}

  /** {@inheritDoc} */
  @Override
  public boolean isEnabled() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public ProfileMetric getOrCreateMetric(MetricName name) {
    Objects.requireNonNull(name, "name");
    return NoopProfileMetric.INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public QueryProfile finish() {
    Map<MetricName, Double> metrics = new LinkedHashMap<>();
    for (MetricName metricName : MetricName.values()) {
      metrics.put(metricName, 0d);
    }
    return new QueryProfile(0d, metrics);
  }
}
