/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/** Maintain the state when {@link Aggregator} iterate on the {@link BindingTuple}. */
public interface AggregationState {
  /** Get {@link ExprValue} result. */
  ExprValue result();
}
