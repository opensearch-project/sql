/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.query;

import java.util.List;
import java.util.Optional;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.client.DataSourceClient;

/** Registry for all query handlers. */
public class QueryHandlerRegistry {

  private final List<QueryHandler<?>> handlers;

  @Inject
  public QueryHandlerRegistry(List<QueryHandler<?>> handlers) {
    this.handlers = handlers;
  }

  /**
   * Finds a handler that can process the given client.
   *
   * @param client The client to find a handler for
   * @param <T> The type of client, extending DataSourceClient
   * @return An optional containing the handler if found
   */
  @SuppressWarnings("unchecked")
  public <T extends DataSourceClient> Optional<QueryHandler<T>> getQueryHandler(T client) {
    return handlers.stream()
        .filter(
            handler -> {
              try {
                // Get the handler's client class and check if it's compatible with our client
                Class<?> handlerClientClass = handler.getClientClass();
                return handlerClientClass.isInstance(client)
                    && ((QueryHandler<T>) handler).canHandle(client);
              } catch (ClassCastException e) {
                return false;
              }
            })
        .map(handler -> (QueryHandler<T>) handler)
        .findFirst();
  }
}
