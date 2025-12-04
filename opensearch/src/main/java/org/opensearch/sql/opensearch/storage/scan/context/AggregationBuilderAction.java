/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

/** A lambda action to apply on the {@link AggPushDownAction} */
public interface AggregationBuilderAction extends AbstractAction<AggPushDownAction> {
  default void pushOperation(PushDownContext context, PushDownOperation operation) {
    // Apply transformation to aggregation builder in the optimization phase as some transformation
    //  may cause exception. We need to detect that exception in advance.
    apply(context.getAggPushDownAction());
    context.getOperationsForAgg().add(operation);
  }
}
