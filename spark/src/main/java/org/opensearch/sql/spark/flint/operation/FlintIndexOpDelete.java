/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/** Flint Index Logical delete operation. Change state to DELETED. */
public class FlintIndexOpDelete extends FlintIndexOp {

  public FlintIndexOpDelete(StateStore stateStore, String datasourceName) {
    super(stateStore, datasourceName);
  }

  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.ACTIVE
        || state == FlintIndexState.EMPTY
        || state == FlintIndexState.DELETING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.DELETING;
  }

  @Override
  void runOp(FlintIndexStateModel flintIndex) {
    // logically delete, do nothing.
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.DELETED;
  }
}
