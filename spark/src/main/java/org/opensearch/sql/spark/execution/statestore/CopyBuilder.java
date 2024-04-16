/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

public interface CopyBuilder<T> {
  T of(T copy, long seqNo, long primaryTerm);
}
