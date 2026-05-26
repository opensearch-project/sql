/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;

/** A lambda action to apply on the {@link OpenSearchRequestBuilder} */
public interface OSRequestBuilderAction extends AbstractAction<OpenSearchRequestBuilder> {
  default void pushOperation(PushDownContext context, PushDownOperation operation) {
    context.addOperationForRequestBuilder(operation);
  }
}
