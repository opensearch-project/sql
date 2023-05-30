/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class DateTimeFormatsIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.DATE_FORMATS);
  }

  @Test
  public void testReadingDateFormats() throws IOException {
    String query = String.format("SELECT weekyear_week_day, hour_minute_second_millis," +
        " strict_ordinal_date_time FROM %s LIMIT 1", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("weekyear_week_day", null, "date"),
        schema("hour_minute_second_millis", null, "time"),
        schema("strict_ordinal_date_time", null, "timestamp"));
    verifyDataRows(result,
        rows("1984-04-12",
            "09:07:42",
            "1984-04-12 09:07:42.000123456"
        ));
  }

  @Test
  public void testDateFormatsWithOr() throws IOException {
    String query = String.format("SELECT yyyy-MM-dd_OR_epoch_millis FROM %s", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifyDataRows(result,
        rows("1984-04-12 00:00:00"),
        rows("1984-04-12 09:07:42.000123456"));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
