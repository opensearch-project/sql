/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_EXPAND_FLATTEN;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

public class ExpandCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.EXPAND_FLATTEN);
  }

  @Test
  public void testBasic() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | expand teams | fields city, teams.name", TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("city", "string"), schema("teams.name", "string"));
    verifyDataRows(
        result,
        rows("Seattle", "Seattle Seahawks"),
        rows("Seattle", "Seattle Kraken"),
        rows("Vancouver", "Vancouver Canucks"),
        rows("Vancouver", "BC Lions"),
        rows("San Antonio", "San Antonio Spurs"),
        rows("Null City", null),
        rows("Missing City", null));
  }

  @Test
  public void testNested() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | where city = 'San Antonio' | expand teams.title | fields teams.name,"
                + " teams.title",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("teams.name", "string"), schema("teams.title", "integer"));
    verifyDataRows(
        result,
        rows("San Antonio Spurs", 1999),
        rows("San Antonio Spurs", 2003),
        rows("San Antonio Spurs", 2005),
        rows("San Antonio Spurs", 2007),
        rows("San Antonio Spurs", 2014));
  }

  @Test
  public void testMultiple() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | expand teams | expand teams.title | fields teams.name, teams.title",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("teams.name", "string"), schema("teams.title", "integer"));
    verifyDataRows(
        result,
        rows("Seattle Seahawks", 2014),
        rows("Seattle Kraken", null),
        rows("Vancouver Canucks", null),
        rows("BC Lions", 1964),
        rows("BC Lions", 1985),
        rows("BC Lions", 1994),
        rows("BC Lions", 2000),
        rows("BC Lions", 2006),
        rows("BC Lions", 2011),
        rows("San Antonio Spurs", 1999),
        rows("San Antonio Spurs", 2003),
        rows("San Antonio Spurs", 2005),
        rows("San Antonio Spurs", 2007),
        rows("San Antonio Spurs", 2014),
        rows(null, null),
        rows(null, null));
  }

  @Test
  public void testExpandFlatten() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | expand teams | flatten teams | fields name, title",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("name", "string"), schema("title", "integer"));
    verifyDataRows(
        result,
        rows("Seattle Seahawks", 2014),
        rows("Seattle Kraken", null),
        rows("Vancouver Canucks", null),
        rows("BC Lions", List.of(1964, 1985, 1994, 2000, 2006, 2011)),
        rows("San Antonio Spurs", List.of(1999, 2003, 2005, 2007, 2014)),
        rows(null, null),
        rows(null, null));
  }
}
