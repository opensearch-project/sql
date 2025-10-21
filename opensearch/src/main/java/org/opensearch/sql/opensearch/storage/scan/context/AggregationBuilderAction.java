/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

public interface AggregationBuilderAction extends AbstractAction<AggPushDownAction> {
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getAggPushDownAction());
    context.getOperationsForAgg().add(operation);
  }
}
