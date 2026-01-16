/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** No-op metric implementation. */
final class NoopProfileMetric implements ProfileMetric {

  static final NoopProfileMetric INSTANCE = new NoopProfileMetric();

  private NoopProfileMetric() {}

  /** {@inheritDoc} */
  @Override
  public String name() {
    return "";
  }

  /** {@inheritDoc} */
  @Override
  public long value() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public void add(long delta) {}

  /** {@inheritDoc} */
  @Override
  public void set(long value) {}
}
