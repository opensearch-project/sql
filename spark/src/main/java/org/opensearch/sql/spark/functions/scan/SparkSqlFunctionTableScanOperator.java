/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.functions.scan;

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
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.response.DefaultSparkSqlFunctionResponseHandle;
import org.opensearch.sql.spark.functions.response.SparkSqlFunctionResponseHandle;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * This a table scan operator to handle sql table function.
 */
@RequiredArgsConstructor
public class SparkSqlFunctionTableScanOperator extends TableScanOperator {
  private final SparkClient sparkClient;
  private final SparkQueryRequest request;
  private SparkSqlFunctionResponseHandle sparkResponseHandle;
  private static final Logger LOG = LogManager.getLogger();

  @Override
  public void open() {
    super.open();
    this.sparkResponseHandle = AccessController.doPrivileged(
        (PrivilegedAction<SparkSqlFunctionResponseHandle>) () -> {
          try {
            JSONObject responseObject = sparkClient.sql(request.getSql());
            return new DefaultSparkSqlFunctionResponseHandle(responseObject);
          } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(
                String.format("Error fetching data from spark server: %s", e.getMessage()));
          }
        });
  }

  @Override
  public boolean hasNext() {
    return this.sparkResponseHandle.hasNext();
  }

  @Override
  public ExprValue next() {
    return this.sparkResponseHandle.next();
  }

  @Override
  public String explain() {
    return String.format(Locale.ROOT, "sql(%s)", request.getSql());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return this.sparkResponseHandle.schema();
  }
}
