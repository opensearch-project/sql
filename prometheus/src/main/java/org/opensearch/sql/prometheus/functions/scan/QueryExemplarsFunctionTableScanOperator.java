/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.functions.scan;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.response.QueryExemplarsFunctionResponseHandle;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * This class is for QueryExemplars function {@link TableScanOperator}. This takes care of getting
 * exemplar data from prometheus by making {@link PrometheusQueryExemplarsRequest}.
 */
@RequiredArgsConstructor
public class QueryExemplarsFunctionTableScanOperator extends TableScanOperator {

  private final PrometheusClient prometheusClient;

  @Getter private final PrometheusQueryExemplarsRequest request;
  private QueryExemplarsFunctionResponseHandle queryExemplarsFunctionResponseHandle;
  private static final Logger LOG = LogManager.getLogger();

  @Override
  public void open() {
    super.open();
    this.queryExemplarsFunctionResponseHandle =
        AccessController.doPrivileged(
            (PrivilegedAction<QueryExemplarsFunctionResponseHandle>)
                () -> {
                  try {
                    JSONArray responseArray =
                        prometheusClient.queryExemplars(
                            request.getQuery(), request.getStartTime(), request.getEndTime());
                    return new QueryExemplarsFunctionResponseHandle(responseArray);
                  } catch (IOException e) {
                    LOG.error(e.getMessage());
                    throw new RuntimeException(
                        String.format(
                            "Error fetching data from prometheus server: %s", e.getMessage()));
                  }
                });
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public boolean hasNext() {
    return this.queryExemplarsFunctionResponseHandle.hasNext();
  }

  @Override
  public ExprValue next() {
    return this.queryExemplarsFunctionResponseHandle.next();
  }

  @Override
  public String explain() {
    return String.format(
        Locale.ROOT,
        "query_exemplars(%s, %s, %s)",
        request.getQuery(),
        request.getStartTime(),
        request.getEndTime());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return queryExemplarsFunctionResponseHandle.schema();
  }
}
