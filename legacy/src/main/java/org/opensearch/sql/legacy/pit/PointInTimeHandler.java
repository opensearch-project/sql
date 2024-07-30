/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

/** Point In Time */
public interface PointInTimeHandler {
  /**
   * Create Point In Time
   *
   * @return Point In Time creation status
   */
  boolean create();

  /**
   * Delete Point In Time
   *
   * @return Point In Time deletion status
   */
  boolean delete();

  /** Get Point In Time Identifier */
  String getPitId();
}
