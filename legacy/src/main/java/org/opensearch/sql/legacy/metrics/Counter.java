/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

public interface Counter<T> {

  void increment();

  void add(long n);

  T getValue();

  void reset();
}
