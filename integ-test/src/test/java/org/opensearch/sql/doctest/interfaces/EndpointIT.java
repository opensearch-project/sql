/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.interfaces;

import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.IGNORE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;

import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;

/**
 * Doc test for endpoints to access the plugin.
 */
@DocTestConfig(template = "interfaces/endpoint.rst", testData = {"accounts.json"})
public class EndpointIT extends DocTest {

  @Section(1)
  public void queryByPost() {
    section(
        title("POST"),
        description("You can also send HTTP POST request with your query in request body."),
        example(
            description(),
            post("SELECT * FROM accounts"),
            queryFormat(CURL_REQUEST, IGNORE_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

  @Section(2)
  public void explainQuery() {
    section(
        title("Explain"),
        description(
            "To translate your query, send it to explain endpoint. The explain output is OpenSearch",
            "domain specific language (DSL) in JSON format. You can just copy and paste it to your",
            "console to run it against OpenSearch directly."
        ),
        example(
            description(),
            post("SELECT firstname, lastname FROM accounts WHERE age > 20"),
            queryFormat(IGNORE_REQUEST, IGNORE_RESPONSE),
            explainFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE)
        )
    );
  }

  @Section(3)
  public void cursorQuery() {
    section(
        title("Cursor"),
        description(
            "To get paginated response for a query, user needs to provide `fetch_size` parameter as part of normal query.",
            "The value of `fetch_size` should be greater than `0`. In absence of `fetch_size`, default value of 1000 is used.",
            "A value of `0` will fallback to non-paginated response.",
            "This feature is only available over `jdbc` format for now."
        ),
        example(
            description(),
            post("SELECT firstname, lastname FROM accounts WHERE age > 20 ORDER BY state ASC"),
            queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

}
