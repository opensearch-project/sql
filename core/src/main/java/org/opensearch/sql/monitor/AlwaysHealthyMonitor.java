/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

/** Always healthy resource monitor. */
public class AlwaysHealthyMonitor extends ResourceMonitor {
  public static final ResourceMonitor ALWAYS_HEALTHY_MONITOR = new AlwaysHealthyMonitor();

  /** always healthy. */
  @Override
  protected boolean isHealthyImpl() {
    return true;
  }

  @Override
  public ResourceStatus getStatus() {
    return ResourceStatus.healthy(ResourceStatus.ResourceType.OTHER);
  }
}
