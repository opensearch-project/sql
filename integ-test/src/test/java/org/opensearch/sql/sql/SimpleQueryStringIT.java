/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class SimpleQueryStringIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  /*
  The 'beer.stackexchange' index is a dump of beer.stackexchange.com converted to the format which might be ingested by OpenSearch.
  This is a forum like StackOverflow with questions about beer brewing. The dump contains both questions, answers and comments.
  The reference query is:
    select count(Id) from beer.stackexchange where simple_query_string(["Tags" ^ 1.5, Title, `Body` 4.2], 'taste') and Tags like '% % %' and Title like '%';
  It filters out empty `Tags` and `Title`.
  */

  @Test
  public void test1() throws IOException {
    String query = "SELECT count(*) FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertNotEquals(0, result.getInt("total"));
  }

  @Test
  public void verify_wildcard_test() throws IOException {
    String query1 = "SELECT count(*) FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string(['Tags'], 'taste')";
    var result1 = new JSONObject(executeQuery(query1, "jdbc"));
    String query2 = "SELECT count(*) FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string(['T*'], 'taste')";
    var result2 = new JSONObject(executeQuery(query2, "jdbc"));
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));
  }
}
