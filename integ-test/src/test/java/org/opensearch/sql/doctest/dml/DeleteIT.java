/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.dml;

import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.OPENSEARCH_DASHBOARD_REQUEST;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;

import org.opensearch.sql.doctest.core.DocTest;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;

@DocTestConfig(template = "dml/delete.rst", testData = {"accounts.json"})
public class DeleteIT extends DocTest {

  @Section(1)
  public void delete() {
    section(
        title("DELETE"),
        description(
            "``DELETE`` statement deletes documents that satisfy the predicates in ``WHERE`` clause.",
            "Note that all documents are deleted in the case of ``WHERE`` clause absent."
        ),
        images("rdd/singleDeleteStatement.png"),
        example(
            description(
                "The ``datarows`` field in this case shows rows impacted, in other words how many",
                "documents were just deleted."
            ),
            post(multiLine(
                "DELETE FROM accounts",
                "WHERE age > 30"
            )),
            queryFormat(OPENSEARCH_DASHBOARD_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, PRETTY_JSON_RESPONSE)
        )
    );
  }

}
