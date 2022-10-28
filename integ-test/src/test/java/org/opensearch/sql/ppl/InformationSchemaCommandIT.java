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
import org.junit.jupiter.api.Test;

public class InformationSchemaCommandIT extends PPLIntegTestCase {

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

}
