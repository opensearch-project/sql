/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class InformationSchemaCommandIT extends PPLIntegTestCase {


  @Override
  protected void init() throws Exception {
    loadIndex(Index.DATASOURCES);
  }

  @Test
  public void testSearchTablesFromPrometheusCatalog() throws IOException {
    JSONObject result =
        executeQuery("source=my_prometheus.information_schema.tables "
            + "| where LIKE(TABLE_NAME, '%http%')");
    this.logger.error(result.toString());
    verifyColumn(
        result,
        columnName("TABLE_CATALOG"),
        columnName("TABLE_SCHEMA"),
        columnName("TABLE_NAME"),
        columnName("TABLE_TYPE"),
        columnName("UNIT"),
        columnName("REMARKS")
    );
    verifyDataRows(result,
        rows("my_prometheus", "default", "promhttp_metric_handler_requests_in_flight",
            "gauge", "", "Current number of scrapes being served."),
        rows("my_prometheus", "default", "prometheus_sd_http_failures_total",
            "counter", "", "Number of HTTP service discovery refresh failures."),
        rows("my_prometheus", "default", "promhttp_metric_handler_requests_total",
            "counter", "", "Total number of scrapes by HTTP status code."),
        rows("my_prometheus", "default", "prometheus_http_request_duration_seconds",
            "histogram", "", "Histogram of latencies for HTTP requests."),
        rows("my_prometheus", "default", "prometheus_http_requests_total",
            "counter", "", "Counter of HTTP requests."),
        rows("my_prometheus", "default", "prometheus_http_response_size_bytes",
            "histogram", "", "Histogram of response size for HTTP requests."));
  }


  @Test
  public void testTablesFromPrometheusCatalog() throws IOException {
    JSONObject result =
        executeQuery(
            "source = my_prometheus.information_schema.tables "
                + "| where TABLE_NAME='prometheus_http_requests_total'");
    this.logger.error(result.toString());
    verifyColumn(
        result,
        columnName("TABLE_CATALOG"),
        columnName("TABLE_SCHEMA"),
        columnName("TABLE_NAME"),
        columnName("TABLE_TYPE"),
        columnName("UNIT"),
        columnName("REMARKS")
    );
    verifyDataRows(result,
        rows("my_prometheus",
            "default", "prometheus_http_requests_total",
            "counter", "", "Counter of HTTP requests."));
  }


  // Moved this IT from DescribeCommandIT to segregate Datasource Integ Tests.
  @Test
  public void testDescribeCommandWithPrometheusCatalog() throws IOException {
    JSONObject result = executeQuery("describe  my_prometheus.prometheus_http_requests_total");
    verifyColumn(
        result,
        columnName("TABLE_CATALOG"),
        columnName("TABLE_SCHEMA"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE")
    );
    verifyDataRows(result,
        rows("my_prometheus", "default", "prometheus_http_requests_total", "handler", "keyword"),
        rows("my_prometheus", "default", "prometheus_http_requests_total", "code", "keyword"),
        rows("my_prometheus", "default", "prometheus_http_requests_total", "instance", "keyword"),
        rows("my_prometheus", "default", "prometheus_http_requests_total", "@value", "double"),
        rows("my_prometheus", "default", "prometheus_http_requests_total", "@timestamp",
            "timestamp"),
        rows("my_prometheus", "default", "prometheus_http_requests_total", "job", "keyword"));
  }

}
