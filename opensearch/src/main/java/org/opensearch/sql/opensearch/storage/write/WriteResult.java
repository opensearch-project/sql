/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Cumulative outcome of a {@link OpenSearchBulkWriter} run: rows written and any tolerated per-item
 * failures.
 */
public record WriteResult(long written, List<ItemFailure> failures) {

  /** A single failed bulk item. */
  public record ItemFailure(int batchPos, @Nullable String id, String reason) {}
}
