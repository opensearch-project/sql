/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import java.util.Collection;
import java.util.Collections;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;


/**
 * Prometheus storage engine implementation.
 */
public class PrometheusStorageEngine implements StorageEngine {

  private final PrometheusClient prometheusClient;

  public PrometheusStorageEngine(PrometheusClient prometheusClient) {
    this.prometheusClient = prometheusClient;
  }

  @Override
  public Table getTable(String name) {
    return null;
  }

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(new QueryRangeTableFunctionResolver(prometheusClient));
  }

}
