/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

public class DescribeCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DOG);
  }

  @Test
  public void testDescribeAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s", TEST_INDEX_DOG));
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN")
    );
  }

  @Test
  public void testDescribeFilterFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s | fields TABLE_NAME, COLUMN_NAME, TYPE_NAME", TEST_INDEX_DOG));
    verifyColumn(
        result,
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("TYPE_NAME")
    );
  }

  @Test
  public void testDescribeWithSpecialIndexName() throws IOException {
    executeRequest(new Request("PUT", "/logs-2021.01.11"));
    verifyDataRows(executeQuery("describe logs-2021.01.11"));

    executeRequest(new Request("PUT", "/logs-7.10.0-2021.01.11"));
    verifyDataRows(executeQuery("describe logs-7.10.0-2021.01.11"));
  }

  @Test
  public void describeCommandWithoutIndexShouldFailToParse() throws IOException {
    try {
      executeQuery("describe");
      fail();
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("RuntimeException"));
      assertTrue(e.getMessage().contains("Failed to parse query due to offending symbol"));
    }
  }

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
