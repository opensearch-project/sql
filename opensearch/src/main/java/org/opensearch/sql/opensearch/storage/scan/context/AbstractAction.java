/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

/**
 * A lambda action to apply on the target T
 *
 * @param <T> the target type
 */
public interface AbstractAction<T> {
  void apply(T target);

  /**
   * Apply the action on the target T and add the operation to the context
   *
   * @param context the context to add the operation to
   * @param operation the operation to add to the context
   */
  void transform(PushDownContext context, PushDownOperation operation);
}
