/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

/** Point In Time */
public interface PointInTimeHandler {
  /** Create Point In Time */
  void create();

  /** Delete Point In Time */
  void delete();

  /** Get Point In Time Identifier */
  String getPitId();
}
