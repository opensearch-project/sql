/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import java.util.function.Consumer;
import org.opensearch.rest.RestChannel;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Runs a SQL execution under a coordinator task so the DSL search tasks it spawns carry a parent
 * reference back to the originating SQL query.
 *
 * <p>The concrete implementation (wired in the plugin module) dispatches through a local transport
 * action that registers the coordinator task; this seam exists because {@code RestSqlAction} lives
 * in the {@code legacy} module and cannot depend on the plugin-module transport action directly.
 * The default implementation simply runs the work with no coordinator task, preserving behavior for
 * callers/tests that do not supply one.
 */
@FunctionalInterface
public interface SqlCoordinatorTaskDispatcher {

  /** Default: run the work directly without establishing a coordinator task. */
  SqlCoordinatorTaskDispatcher PASSTHROUGH = (client, query, channel, work) -> work.accept(channel);

  /**
   * @param client node client used to dispatch the local coordinator-task action
   * @param query original SQL query text (recorded on the coordinator task)
   * @param channel REST channel the execution writes its response to
   * @param work the SQL execution to run under the coordinator task
   */
  void dispatch(NodeClient client, String query, RestChannel channel, Consumer<RestChannel> work);
}
