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

package org.opensearch.sql.doctest.admin;

import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.IGNORE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;
import static org.opensearch.sql.legacy.metrics.MetricName.DEFAULT_CURSOR_REQUEST_COUNT_TOTAL;
import static org.opensearch.sql.legacy.metrics.MetricName.DEFAULT_CURSOR_REQUEST_TOTAL;
import static org.opensearch.sql.legacy.metrics.MetricName.FAILED_REQ_COUNT_CB;
import static org.opensearch.sql.legacy.metrics.MetricName.FAILED_REQ_COUNT_CUS;
import static org.opensearch.sql.legacy.metrics.MetricName.FAILED_REQ_COUNT_SYS;
import static org.opensearch.sql.legacy.metrics.MetricName.REQ_COUNT_TOTAL;
import static org.opensearch.sql.legacy.metrics.MetricName.REQ_TOTAL;
import static org.opensearch.sql.legacy.plugin.RestSqlStatsAction.STATS_API_ENDPOINT;

import org.opensearch.sql.doctest.core.DocTest;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;
import org.opensearch.sql.doctest.core.builder.Requests;
import org.opensearch.sql.doctest.core.request.SqlRequest;
import org.opensearch.sql.doctest.core.response.DataTable;
import org.opensearch.sql.legacy.metrics.MetricName;

/**
 * Doc test for plugin monitoring functionality
 */
@DocTestConfig(template = "admin/monitoring.rst")
public class MonitoringIT extends DocTest {

  @Section
  public void nodeStats() {
    section(
        title("Node Stats"),
        description(
            "The meaning of fields in the response is as follows:\n\n" + fieldDescriptions()),
        example(
            description(),
            getStats(),
            queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
            explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE)
        )
    );
  }

  private String fieldDescriptions() {
    DataTable table = new DataTable(new String[] {"Field name", "Description"});
    table.addRow(row(REQ_TOTAL, "Total count of request"));
    table.addRow(row(REQ_COUNT_TOTAL, "Total count of request within the interval"));
    table.addRow(row(DEFAULT_CURSOR_REQUEST_TOTAL, "Total count of simple cursor request"));
    table.addRow(row(DEFAULT_CURSOR_REQUEST_COUNT_TOTAL,
        "Total count of simple cursor request within the interval"));
    table.addRow(row(FAILED_REQ_COUNT_SYS,
        "Count of failed request due to system error within the interval"));
    table.addRow(row(FAILED_REQ_COUNT_CUS,
        "Count of failed request due to bad request within the interval"));
    table.addRow(
        row(FAILED_REQ_COUNT_CB, "Indicate if plugin is being circuit broken within the interval"));

    return table.toString();
  }

  private String[] row(MetricName name, String description) {
    return new String[] {name.getName(), description};
  }

  private Requests getStats() {
    return new Requests(restClient(), new SqlRequest("GET", STATS_API_ENDPOINT, ""));
  }

}
