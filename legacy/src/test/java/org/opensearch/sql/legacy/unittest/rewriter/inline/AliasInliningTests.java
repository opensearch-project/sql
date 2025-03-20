/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.inline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockLocalClusterState;
import static org.opensearch.sql.legacy.util.SqlParserUtils.parse;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.query.AggregationQueryAction;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.transport.client.Client;

public class AliasInliningTests {

  private static final String TEST_MAPPING_FILE = "mappings/semantics.json";

  @Before
  public void setUp() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String mappings = Resources.toString(url, Charsets.UTF_8);
    mockLocalClusterState(mappings);
  }

  @Test
  public void orderByAliasedFieldTest() throws SqlParseException {
    String originalQuery =
        "SELECT utc_time date "
            + "FROM opensearch_dashboards_sample_data_logs "
            + "ORDER BY date DESC";
    String originalDsl = parseAsSimpleQuery(originalQuery);

    String rewrittenQuery =
        "SELECT utc_time date "
            + "FROM opensearch_dashboards_sample_data_logs "
            + "ORDER BY utc_time DESC";

    String rewrittenDsl = parseAsSimpleQuery(rewrittenQuery);

    assertThat(originalDsl, equalTo(rewrittenDsl));
  }

  @Test
  public void orderByAliasedScriptedField() throws SqlParseException {
    String originalDsl =
        parseAsSimpleQuery(
            "SELECT date_format(birthday, 'dd-MM-YYYY') date " + "FROM bank " + "ORDER BY date");
    String rewrittenQuery =
        "SELECT date_format(birthday, 'dd-MM-YYYY') date "
            + "FROM bank "
            + "ORDER BY date_format(birthday, 'dd-MM-YYYY')";

    String rewrittenDsl = parseAsSimpleQuery(rewrittenQuery);
    assertThat(originalDsl, equalTo(rewrittenDsl));
  }

  @Test
  public void groupByAliasedFieldTest() throws SqlParseException {
    String originalQuery =
        "SELECT utc_time date " + "FROM opensearch_dashboards_sample_data_logs " + "GROUP BY date";

    String originalDsl = parseAsAggregationQuery(originalQuery);

    String rewrittenQuery =
        "SELECT utc_time date "
            + "FROM opensearch_dashboards_sample_data_logs "
            + "GROUP BY utc_time DESC";

    String rewrittenDsl = parseAsAggregationQuery(rewrittenQuery);

    assertThat(originalDsl, equalTo(rewrittenDsl));
  }

  @Test
  public void groupAndSortBySameExprAlias() throws SqlParseException {
    String query =
        "SELECT date_format(timestamp, 'yyyy-MM') opensearch-table.timestamp_tg, COUNT(*) count,"
            + " COUNT(DistanceKilometers) opensearch-table.DistanceKilometers_count\n"
            + "FROM opensearch_dashboards_sample_data_flights\n"
            + "GROUP BY date_format(timestamp, 'yyyy-MM')\n"
            + "ORDER BY date_format(timestamp, 'yyyy-MM') DESC\n"
            + "LIMIT 2500";
    String dsl = parseAsAggregationQuery(query);

    JSONObject parseQuery = new JSONObject(dsl);

    assertThat(
        parseQuery.query("/aggregations/opensearch-table.timestamp_tg/terms/script"),
        notNullValue());
  }

  @Test
  public void groupByAndSortAliased() throws SqlParseException {
    String dsl =
        parseAsAggregationQuery(
            "SELECT date_format(utc_time, 'dd-MM-YYYY') date "
                + "FROM opensearch_dashboards_sample_data_logs "
                + "GROUP BY date "
                + "ORDER BY date DESC");

    JSONObject parsedQuery = new JSONObject(dsl);

    JSONObject query = (JSONObject) parsedQuery.query("/aggregations/date/terms/script");

    assertThat(query, notNullValue());
  }

  private String parseAsSimpleQuery(String originalQuery) throws SqlParseException {
    SqlRequest sqlRequest = new SqlRequest(originalQuery, new JSONObject());
    DefaultQueryAction defaultQueryAction =
        new DefaultQueryAction(
            mock(Client.class), new SqlParser().parseSelect(parse(originalQuery)));
    defaultQueryAction.setSqlRequest(sqlRequest);
    return defaultQueryAction.explain().explain();
  }

  private String parseAsAggregationQuery(String originalQuery) throws SqlParseException {
    return new AggregationQueryAction(
            mock(Client.class), new SqlParser().parseSelect(parse(originalQuery)))
        .explain()
        .explain();
  }
}
