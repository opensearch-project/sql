/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestsConstants;

public class CalcitePPLCaseFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.WEBLOG);
    appendDataForBadResponse();
  }

  private void appendDataForBadResponse() throws IOException {
    Request request1 = new Request("PUT", "/" + TEST_INDEX_WEBLOGS + "/_doc/7?refresh=true");
    request1.setJsonEntity(
        "{\"host\": \"::1\", \"method\": \"GET\", \"url\": \"/history/apollo/\", \"response\":"
            + " \"301\", \"bytes\": \"6245\"}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/8?refresh=true");
    request2.setJsonEntity(
        "{\"host\": \"0.0.0.2\", \"method\": \"GET\", \"url\":"
            + " \"/shuttle/missions/sts-73/mission-sts-73.html\", \"response\": \"500\", \"bytes\":"
            + " \"4085\"}");
    client().performRequest(request2);
    Request request3 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/9?refresh=true");
    request3.setJsonEntity(
        "{\"host\": \"::3\", \"method\": \"GET\", \"url\": \"/shuttle/countdown/countdown.html\","
            + " \"response\": \"403\", \"bytes\": \"3985\"}");
    client().performRequest(request3);
    Request request4 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/10?refresh=true");
    request4.setJsonEntity(
        "{\"host\": \"1.2.3.5\", \"method\": \"GET\", \"url\": \"/history/voyager2/\","
            + " \"response\": null, \"bytes\": \"4321\"}");
    client().performRequest(request4);
  }

  @Test
  public void testCaseWhenWithCast() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval status =
                        case(
                            cast(response as int) >= 200 AND cast(response as int) < 300, "Success",
                            cast(response as int) >= 300 AND cast(response as int) < 400, "Redirection",
                            cast(response as int) >= 400 AND cast(response as int) < 500, "Client Error",
                            cast(response as int) >= 500 AND cast(response as int) < 600, "Server Error"
                            else concat("Incorrect HTTP status code for", url))
                    | where status != "Success"
                    | fields host, method, bytes, response, url, status
                    """,
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows("::3", "GET", "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows(
            "1.2.3.5",
            "GET",
            "4321",
            null,
            "/history/voyager2/",
            "Incorrect HTTP status code for/history/voyager2/"));
  }

  @Test
  public void testCaseWhenNoElse() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval status =
                        case(
                            cast(response as int) >= 200 AND cast(response as int) < 300, "Success",
                            cast(response as int) >= 300 AND cast(response as int) < 400, "Redirection",
                            cast(response as int) >= 400 AND cast(response as int) < 500, "Client Error",
                            cast(response as int) >= 500 AND cast(response as int) < 600, "Server Error")
                    | where isnull(status) OR status != "Success"
                    | fields host, method, bytes, response, url, status
                    """,
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows("::3", "GET", "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows("1.2.3.5", "GET", "4321", null, "/history/voyager2/", null));
  }

  @Test
  public void testCaseWhenWithIn() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval status =
                        case(
                            response in ('200'), "Success",
                            response in ('300', '301'), "Redirection",
                            response in ('400', '403'), "Client Error",
                            response in ('500', '505'), "Server Error"
                            else concat("Incorrect HTTP status code for", url))
                    | where status != "Success"
                    | fields host, method, bytes, response, url, status
                    """,
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows("::3", "GET", "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows(
            "1.2.3.5",
            "GET",
            "4321",
            null,
            "/history/voyager2/",
            "Incorrect HTTP status code for/history/voyager2/"));
  }

  @Test
  public void testCaseWhenInFilter() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | where not true =
                        case(
                            response in ('200'), true,
                            response in ('300', '301'), false,
                            response in ('400', '403'), false,
                            response in ('500', '505'), false
                            else false)
                    | fields host, method, bytes, response, url
                    """,
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", "6245", "301", "/history/apollo/"),
        rows("0.0.0.2", "GET", "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html"),
        rows("::3", "GET", "3985", "403", "/shuttle/countdown/countdown.html"),
        rows("1.2.3.5", "GET", "4321", null, "/history/voyager2/"));
  }

  @Test
  public void testCaseWhenInSubquery() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | where response in [
                        source = %s
                        | eval new_response = case(
                            response in ('200'), "201",
                            response in ('300', '301'), "301",
                            response in ('400', '403'), "403",
                            response in ('500', '505'), "500"
                            else concat("Incorrect HTTP status code for", url))
                        | fields new_response
                      ]
                    | fields host, method, bytes, response, url
                    """,
                TEST_INDEX_WEBLOGS, TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", "6245", "301", "/history/apollo/"),
        rows("0.0.0.2", "GET", "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html"),
        rows("::3", "GET", "3985", "403", "/shuttle/countdown/countdown.html"));
  }
}
