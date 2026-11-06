/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

/**
 * Base interface for all data source clients. This interface serves as a marker interface for all
 * client implementations.
 *
 * @opensearch.experimental
 */
public interface DataSourceClient {
  /**
   * Releases whatever transport resources this client holds. Clients are cached by {@code
   * DataSourceClientFactory}, so an evicted or replaced client is otherwise unreachable while its
   * OkHttp dispatcher thread pool and connection pool stay alive until OkHttp's own idle timers
   * expire.
   *
   * <p>A default no-op keeps this a marker interface for implementations with nothing to release.
   */
  default void close() {}
}
