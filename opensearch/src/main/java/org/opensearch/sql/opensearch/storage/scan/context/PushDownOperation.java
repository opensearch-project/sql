/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a push down operation that can be applied to an OpenSearchRequestBuilder.
 */
@EqualsAndHashCode
public class PushDownOperation {
  private final PushDownType type;
  private final Object digest;
  private final AbstractAction<?> action;

  public PushDownOperation(PushDownType type, Object digest, AbstractAction<?> action) {
    this.type = type;
    this.digest = digest;
    this.action = action;
  }

  public PushDownType type() {
    return type;
  }

  public Object digest() {
    return digest;
  }

  public AbstractAction<?> action() {
    return action;
  }

  @Override
  public String toString() {
    return type + "->" + digest;
  }
}
