/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.doctest.core.test;

import static com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequestFormat.OPENSEARCH_DASHBOARD_REQUEST;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequest;
import com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequest.UrlParam;
import com.amazon.opendistroforelasticsearch.sql.doctest.core.request.SqlRequestFormat;
import org.junit.Test;

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
        ">> curl -H 'Content-Type: application/json' -X POST localhost:9200/_opensearch/_sql?format=jdbc -d '{\n" +
            "  \"query\" : \"SELECT * FROM accounts\"\n" +
            "}'";
    assertThat(CURL_REQUEST.format(sqlRequest), is(expected));
  }

  @Test
  public void testOpenSearchDashboardsFormat() {
    String expected =
        "POST /_opensearch/_sql?format=jdbc\n" +
            "{\n" +
            "  \"query\" : \"SELECT * FROM accounts\"\n" +
            "}";
    assertThat(OPENSEARCH_DASHBOARD_REQUEST.format(sqlRequest), is(expected));
  }

  @Test
  public void multiLineSqlInOpenSearchDashboardRequestShouldBeWellFormatted() {
    SqlRequest multiLineSqlRequest = new SqlRequest(
        "POST",
        "/_opensearch/_sql",
        "{\"query\":\"SELECT *\\nFROM accounts\\nWHERE age > 30\"}"
    );

    String expected =
        "POST /_opensearch/_sql\n" +
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
