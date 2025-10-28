/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

/**
 * Represents a push down operation that can be applied to an OpenSearchRequestBuilder.
 *
 * @param type PushDownType enum
 * @param digest the digest of the pushed down operator
 * @param action the lambda action to apply on the OpenSearchRequestBuilder
 */
public record PushDownOperation(PushDownType type, Object digest, AbstractAction<?> action) {
  public String toString() {
    return type + "->" + digest;
  }
}
