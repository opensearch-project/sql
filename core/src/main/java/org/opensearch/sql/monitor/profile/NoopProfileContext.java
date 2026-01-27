/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.Objects;

/** Disabled profiling context. */
public final class NoopProfileContext implements ProfileContext {

  public static final NoopProfileContext INSTANCE = new NoopProfileContext();

  private NoopProfileContext() {}

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

  @Override
  public void setPlanRoot(ProfilePlanNode planRoot) {}

  /** {@inheritDoc} */
  @Override
  public QueryProfile finish() {
    return null;
  }
}
