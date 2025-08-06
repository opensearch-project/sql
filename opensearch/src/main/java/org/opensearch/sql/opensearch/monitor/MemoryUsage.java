/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

/** Memory usage interface. It is used to get the memory usage of the VM. */
public interface MemoryUsage {
  long usage();

  void setUsage(long usage);
}
