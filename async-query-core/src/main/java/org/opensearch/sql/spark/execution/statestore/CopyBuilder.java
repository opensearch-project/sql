/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import com.google.common.collect.ImmutableMap;

/** Interface for copying StateModel object. Refer {@link StateStore} */
public interface CopyBuilder<T> {
  T of(T copy, ImmutableMap<String, Object> metadata);
}
