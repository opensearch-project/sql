/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.OPENSEARCH_DASHBOARD_REQUEST;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import org.junit.Test;
import org.opensearch.sql.doctest.core.request.SqlRequest;
import org.opensearch.sql.doctest.core.request.SqlRequest.UrlParam;
import org.opensearch.sql.doctest.core.request.SqlRequestFormat;

/**
 * Test cases for {@link SqlRequestFormat}
 */
public class SqlRequestFormatTest {

  private final SqlRequest sqlRequest = new SqlRequest(
      "POST",
      QUERY_API_ENDPOINT,
      "{\"query\":\"SELECT * FROM accounts\"}",
      new UrlParam("format", "jdbc")
  );

  @Test
  public void testIgnoreRequestFormat() {
    assertThat(IGNORE_REQUEST.format(sqlRequest), emptyString());
  }

  @Test
  public void testCurlFormat() {
    String expected =
        ">> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=jdbc -d '{\n" +
            "  \"query\" : \"SELECT * FROM accounts\"\n" +
            "}'";
    assertThat(CURL_REQUEST.format(sqlRequest), is(expected));
  }

  @Test
  public void testOpenSearchDashboardsFormat() {
    String expected =
        "POST /_plugins/_sql?format=jdbc\n" +
            "{\n" +
            "  \"query\" : \"SELECT * FROM accounts\"\n" +
            "}";
    assertThat(OPENSEARCH_DASHBOARD_REQUEST.format(sqlRequest), is(expected));
  }

  @Test
  public void multiLineSqlInOpenSearchDashboardRequestShouldBeWellFormatted() {
    SqlRequest multiLineSqlRequest = new SqlRequest(
        "POST",
        "/_plugins/_sql",
        "{\"query\":\"SELECT *\\nFROM accounts\\nWHERE age > 30\"}"
    );

    String expected =
        "POST /_plugins/_sql\n" +
            "{\n" +
            "  \"query\" : \"\"\"\n" +
            "\tSELECT *\n" +
            "\tFROM accounts\n" +
            "\tWHERE age > 30\n" +
            "\t\"\"\"\n" +
            "}";
    assertThat(OPENSEARCH_DASHBOARD_REQUEST.format(multiLineSqlRequest), is(expected));
  }

}
