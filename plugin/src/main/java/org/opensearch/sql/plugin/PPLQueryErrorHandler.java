/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;

/** Classifies PPL failures and records the corresponding customer or system error metric. */
public final class PPLQueryErrorHandler {
  private static final Logger LOG = LogManager.getLogger(PPLQueryErrorHandler.class);

  private PPLQueryErrorHandler() {}

  /**
   * Records a PPL failure and returns the HTTP status associated with it.
   *
   * @param exception query failure
   * @return client or system error status
   */
  public static RestStatus recordFailure(Exception exception) {
    int code = rawStatusCode(exception);
    if (400 <= code && code < 500) {
      increment(MetricName.PPL_FAILED_REQ_COUNT_CUS);
    } else if (500 <= code && code < 600) {
      increment(MetricName.PPL_FAILED_REQ_COUNT_SYS);
    } else {
      LOG.warn(
          "Got an exception returning non-error status {}", RestStatus.fromCode(code), exception);
    }
    return RestStatus.fromCode(code);
  }

  private static int rawStatusCode(Exception exception) {
    if (exception instanceof ErrorReport errorReport) {
      return rawStatusCode(errorReport.getCause());
    }
    if (exception instanceof OpenSearchException openSearchException) {
      return openSearchException.status().getStatus();
    }
    return isClientError(exception) ? 400 : 500;
  }

  private static boolean isClientError(Exception exception) {
    return exception instanceof IllegalArgumentException
        || exception instanceof IndexNotFoundException
        || exception instanceof QueryEngineException
        || exception instanceof SyntaxCheckException
        || exception instanceof DataSourceClientException
        || exception instanceof IllegalAccessException;
  }

  private static void increment(MetricName metricName) {
    Metrics.getInstance().getNumericalMetric(metricName).increment();
  }
}
