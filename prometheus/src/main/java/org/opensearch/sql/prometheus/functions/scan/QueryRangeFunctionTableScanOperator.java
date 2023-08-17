/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.scan;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.response.PrometheusFunctionResponseHandle;
import org.opensearch.sql.prometheus.functions.response.QueryRangeFunctionResponseHandle;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

/** This a table scan operator to handle Query Range table function. */
@RequiredArgsConstructor
public class QueryRangeFunctionTableScanOperator extends TableScanOperator {

  private final PrometheusClient prometheusClient;

  private final PrometheusQueryRequest request;
  private PrometheusFunctionResponseHandle prometheusResponseHandle;

  private static final Logger LOG = LogManager.getLogger();

  @Override
  public void open() {
    super.open();
    this.prometheusResponseHandle =
        AccessController.doPrivileged(
            (PrivilegedAction<PrometheusFunctionResponseHandle>)
                () -> {
                  try {
                    JSONObject responseObject =
                        prometheusClient.queryRange(
                            request.getPromQl(),
                            request.getStartTime(),
                            request.getEndTime(),
                            request.getStep());
                    return new QueryRangeFunctionResponseHandle(responseObject);
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
    return this.prometheusResponseHandle.hasNext();
  }

  @Override
  public ExprValue next() {
    return this.prometheusResponseHandle.next();
  }

  @Override
  public String explain() {
    return String.format(
        Locale.ROOT,
        "query_range(%s, %s, %s, %s)",
        request.getPromQl(),
        request.getStartTime(),
        request.getEndTime(),
        request.getStep());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return this.prometheusResponseHandle.schema();
  }
}
