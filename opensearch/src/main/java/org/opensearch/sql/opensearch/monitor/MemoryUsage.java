/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

public interface MemoryUsage {
  long usage();

  void setUsage(long usage);
}
