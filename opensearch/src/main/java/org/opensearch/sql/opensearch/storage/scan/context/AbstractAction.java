/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

public interface AbstractAction<T> {
  void apply(T target);

  void transform(PushDownContext context, PushDownOperation operation);
}
