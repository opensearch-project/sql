/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner;

/**
 * Enum representing supported index scan modes
 */
public enum IndexScanType {

  /**
   * default regular request
   */
  DEFAULT,

  /**
   * scroll request
   */
  SCROLL,

  /**
   * fetch all data
   */
  FETCH_ALL
}
