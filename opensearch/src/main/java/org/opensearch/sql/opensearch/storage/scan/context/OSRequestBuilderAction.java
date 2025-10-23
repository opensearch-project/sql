/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;

/** A lambda action to apply on the {@link OpenSearchRequestBuilder} */
public interface OSRequestBuilderAction extends AbstractAction<OpenSearchRequestBuilder> {
  /**
   * Apply the action on the target {@link OpenSearchRequestBuilder} and add the operation to the
   * context
   *
   * @param context the context to add the operation to
   * @param operation the operation to add to the context
   */
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getRequestBuilder());
    context.getOperationsForRequestBuilder().add(operation);
  }
}
