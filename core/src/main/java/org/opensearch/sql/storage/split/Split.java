/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.split;

import org.opensearch.sql.storage.StorageEngine;

/**
 * Split is a sections of a data set. Each {@link StorageEngine} should have specific implementation
 * of Split.
 */
public interface Split {

  /**
   * Get the split id.
   *
   * @return split id.
   */
  String getSplitId();
}
