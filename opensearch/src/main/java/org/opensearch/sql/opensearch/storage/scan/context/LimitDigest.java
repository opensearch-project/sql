/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

public record LimitDigest(int limit, int offset) {
  @Override
  public String toString() {
    return offset == 0 ? String.valueOf(limit) : "[" + limit + " from " + offset + "]";
  }
}
