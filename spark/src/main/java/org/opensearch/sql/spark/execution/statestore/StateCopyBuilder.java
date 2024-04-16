/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

public interface StateCopyBuilder<T extends StateModel, S> {
  T of(T copy, S state, long seqNo, long primaryTerm);
}
