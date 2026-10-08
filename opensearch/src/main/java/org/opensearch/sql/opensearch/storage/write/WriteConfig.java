/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.List;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;

/**
 * Configuration for {@link OpenSearchBulkWriter}. Describes the destination, the row schema, and
 * how rows map to documents. Pure write-side concern: it carries no read, PIT, or index-lifecycle
 * state.
 */
public record WriteConfig(
    String destIndex,
    List<String> fields,
    WriteMode mode,
    List<String> keyFields,
    int batchSize,
    RefreshPolicy refresh) {

  public enum WriteMode {
    /** Auto-generated document id: every row is a new document. */
    APPEND,
    /** Explicit document id derived from {@code keyFields}: same key updates in place (upsert). */
    UPSERT
  }

  public WriteConfig {
    if (mode == WriteMode.UPSERT && (keyFields == null || keyFields.isEmpty())) {
      throw new IllegalArgumentException("UPSERT mode requires at least one keyField");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
  }
}
