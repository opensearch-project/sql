/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * View configuration.
 */
@Getter
@RequiredArgsConstructor
@ToString
public class ViewConfig {

  /**
   * View refresh mode.
   */
  public enum RefreshMode {
    MANUAL, // P0: Refresh manually by refresh API
    AUTO;   // P1: Automatic compute the delta and refresh incrementally
  }

  /**
   * View storage distribution.
   */
  public enum DistributeOption {
    EVEN, // even distribution by Round-Robin
    KEY,  // distribution by specific key
    ALL;  // single shard fully replicated to other nodes
  }

  private final RefreshMode refreshMode;
  private final DistributeOption distribution;

}
