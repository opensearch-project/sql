/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.SchemaProvider;

/**
 * SPI for frontend plugins (e.g., opensearch-sql) that integrate with analytics-engine.
 *
 * <p>Implementers are discovered by {@code AnalyticsPlugin} via {@link
 * org.opensearch.plugins.ExtensiblePlugin#loadExtensions}; analytics-engine pushes its services to
 * each consumer once Guice has constructed them. Mirrors the {@code JobSchedulerExtension} pattern
 * from opensearch-job-scheduler — the consumer plugin declares its capability via this interface;
 * the publishing plugin (analytics-engine) handles discovery and lifecycle.
 *
 * <p>This interface lets a frontend declare analytics-engine as an OPTIONAL extended plugin ({@code
 * extendedPlugins = ['analytics-engine;optional=true']}). When analytics-engine is not installed,
 * no consumer ever receives a callback; analytics-routing code paths stay inert and the frontend
 * plugin boots normally.
 *
 * <p><b>Lifecycle.</b> Each setter is invoked exactly once per consumer per node lifecycle, AFTER
 * the node Guice injector is built (i.e., after every plugin's {@code createComponents} returns)
 * and BEFORE the first analytics query is dispatched. Implementations should stash the references
 * for later use; do not assume the services are available during {@code createComponents}.
 *
 * <p><b>NOTE</b> — placeholder location. This interface is drafted in the SQL plugin source tree
 * only to share concrete Java with the analytics-engine team during the SPI bring-up. Its permanent
 * home is {@code sandbox/libs/analytics-framework/.../org/opensearch/analytics/spi/} in the
 * OpenSearch repo. Once the analytics-framework JAR ships this interface, this file is deleted and
 * the SQL plugin picks up the type from the published JAR — the FQN {@code
 * org.opensearch.analytics.spi.AnalyticsFrontEndExtension} is identical, so {@code SQLPlugin}'s
 * {@code implements} clause does not change.
 *
 * @opensearch.internal
 */
public interface AnalyticsFrontEndExtension {

  /**
   * Receives the analytics-engine query plan executor. Called exactly once after the executor is
   * constructed and before any analytics query is dispatched. The executor is safe to invoke from
   * any thread once received.
   */
  void setQueryPlanExecutor(QueryPlanExecutor<RelNode, Iterable<Object[]>> executor);

  /**
   * Receives the analytics-engine schema provider, used to build a Calcite {@code SchemaPlus} from
   * the current cluster state. Called exactly once, with the same lifecycle guarantees as {@link
   * #setQueryPlanExecutor}. The provider is safe to invoke from any thread once received.
   */
  void setSchemaProvider(SchemaProvider schemaProvider);
}
