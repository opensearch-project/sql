/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_EXPAND;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

public class ExpandCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.EXPAND);
  }

  @Test
  public void testBasic() throws IOException {
    String query =
        StringUtils.format("source=%s | expand team | fields city, team.name", TEST_INDEX_EXPAND);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("city", "string"), schema("team.name", "string"));
    verifyDataRows(
        result,
        rows("Seattle", "Seattle Seahawks"),
        rows("Seattle", "Seattle Kraken"),
        rows("Vancouver", "Vancouver Canucks"),
        rows("Vancouver", "BC Lions"),
        rows("San Antonio", "San Antonio Spurs"),
        rows("Empty Sports Team", null),
        rows("Null Sports Team", null),
        rows("Missing Sports Team", null));
  }

  @Test
  public void testNested() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | where city = 'San Antonio' | expand team.title | fields team.name,"
                + " team.title",
            TEST_INDEX_EXPAND);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("team.name", "string"), schema("team.title", "integer"));
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
            "source=%s | expand team | expand team.title | fields team.name, team.title",
            TEST_INDEX_EXPAND);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("team.name", "string"), schema("team.title", "integer"));
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
        rows(null, null),
        rows(null, null));
  }

  @Test
  public void testExpandFlatten() throws IOException {

    // TODO #3016: Test once flatten merged.
  }
}
