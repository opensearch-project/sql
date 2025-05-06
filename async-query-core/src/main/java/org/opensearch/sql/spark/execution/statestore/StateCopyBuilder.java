/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import com.google.common.collect.ImmutableMap;

public interface StateCopyBuilder<T extends StateModel, S> {
  T of(T copy, S state, ImmutableMap<String, Object> metadata);
}
