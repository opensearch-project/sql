/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;

public interface OSRequestBuilderAction extends AbstractAction<OpenSearchRequestBuilder> {
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getRequestBuilder());
    context.getOperationsForRequestBuilder().add(operation);
  }
}
