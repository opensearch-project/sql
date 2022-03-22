/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.interfaces;

import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.IGNORE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.ORIGINAL_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;

import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;

/**
 * Doc test for plugin supported protocols.
 */
@DocTestConfig(template = "interfaces/protocol.rst", testData = {"accounts.json"})
public class ProtocolIT extends DocTest {

  @Section(1)
  public void requestFormat() {
    section(
        title("Request Format"),
        description(
            "The body of HTTP POST request can take a few more other fields with SQL query."),
        example(
            description(
                "Use `filter` to add more conditions to OpenSearch DSL directly."
            ),
            post(
                body(
                    "\"query\": \"SELECT firstname, lastname, balance FROM accounts\"",
                    "\"filter\":{\"range\":{\"balance\":{\"lt\":10000}}}"
                )
            ),
            queryFormat(CURL_REQUEST, IGNORE_RESPONSE),
            explainFormat(IGNORE_REQUEST, PRETTY_JSON_RESPONSE)
        ),
        example(
            description("Use `parameters` for actual parameter value in prepared SQL query."),
            post(
                body(
                    "\"query\": \"SELECT * FROM accounts WHERE age = ?\"",
                    "\"parameters\": [{\"type\": \"integer\", \"value\": 30}]"
                )
            ),
            queryFormat(CURL_REQUEST, IGNORE_RESPONSE),
            explainFormat(IGNORE_REQUEST, PRETTY_JSON_RESPONSE)
        )
    );
  }

  @Section(2)
  public void responseInJDBCFormat() {
    section(
        title("JDBC Format"),
        description(
            "By default the plugin return JDBC format. JDBC format is provided for JDBC driver and client side that needs both schema and",
            "result set well formatted."
        ),
        example(
            description(
                "Here is an example for normal response. The `schema` includes field name and its type",
                "and `datarows` includes the result set."
            ),
            post("SELECT firstname, lastname, age FROM accounts ORDER BY age LIMIT 2"),
            queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        ),
        example(
            description(
                "If any error occurred, error message and the cause will be returned instead."),
            post("SELECT unknown FROM accounts", params("format=jdbc")),
            queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

  @Section(3)
  public void originalDSLResponse() {
    section(
        title("OpenSearch DSL"),
        description(
            "The plugin returns original response from OpenSearch in JSON. Because this is",
            "the native response from OpenSearch, extra efforts are needed to parse and interpret it."
        ),
        example(
            description(),
            post("SELECT firstname, lastname, age FROM accounts ORDER BY age LIMIT 2",
                params("format=json")),
            queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

  @Section(4)
  public void responseInCSVFormat() {
    section(
        title("CSV Format"),
        description("You can also use CSV format to download result set as CSV."),
        example(
            description(),
            post("SELECT firstname, lastname, age FROM accounts ORDER BY age",
                params("format=csv")),
            queryFormat(CURL_REQUEST, ORIGINAL_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

  @Section(5)
  public void responseInRawFormat() {
    section(
        title("Raw Format"),
        description(
            "Additionally raw format can be used to pipe the result to other command line tool for post processing."
        ),
        example(
            description(),
            post("SELECT firstname, lastname, age FROM accounts ORDER BY age",
                params("format=raw")),
            queryFormat(CURL_REQUEST, ORIGINAL_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

}
