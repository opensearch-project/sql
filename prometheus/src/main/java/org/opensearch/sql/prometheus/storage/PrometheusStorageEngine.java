/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * OpenSearch storage engine implementation.
 */
@RequiredArgsConstructor
public class PrometheusStorageEngine implements StorageEngine {

  private final PrometheusClient prometheusService;

  private final PrometheusConfig prometheusConfig;

  @Override
  public Table getTable(String name) {
    return new PrometheusIndex(prometheusService, name, prometheusConfig);
  }
}
