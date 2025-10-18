/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import lombok.Getter;

/**
 * A logical column to represents status of different types of columns while project pushdown. There
 * are three types of selected column. 1. PHYSICAL: Represents the physical scan field that
 * naturally exists in index 2. DERIVED_EXISTING: Represents the pushed down derived field 3.
 * DERIVED_NEW: Represents the new discovered derived field that can be pushed down
 */
public final class SelectedColumn {

  public enum Kind {
    PHYSICAL,
    DERIVED_EXISTING,
    DERIVED_NEW
  }

  @Getter private final Kind kind;
  @Getter private final int oldIdx;
  @Getter private final int projIdx;

  private SelectedColumn(Kind kind, int oldIdx, int projIdx) {
    this.kind = kind;
    this.oldIdx = oldIdx;
    this.projIdx = projIdx;
  }

  /** Generate physical input columns from scan with old index in scan */
  public static SelectedColumn physical(int oldIdx) {
    return new SelectedColumn(Kind.PHYSICAL, oldIdx, -1);
  }

  /** Generate pushed down derived field that already exists with old index in scan */
  public static SelectedColumn derivedExisting(int oldIdx) {
    return new SelectedColumn(Kind.DERIVED_EXISTING, oldIdx, -1);
  }

  /** Generate new derived field to be pushed with original index in project nodes */
  public static SelectedColumn derivedNew(int projIdx) {
    return new SelectedColumn(Kind.DERIVED_NEW, -1, projIdx);
  }
}
