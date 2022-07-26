/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import lombok.Getter;
import lombok.Setter;

public class PlanContext {
  @Getter
  @Setter
  private IndexScanType indexScanType = IndexScanType.QUERY;

  @Getter
  @Setter
  private Integer maxResultWindow = 10000;

  public enum IndexScanType {
    /**
     * default query request
     */
    QUERY,

    /**
     * scroll request
     */
    SCROLL
  }
}
