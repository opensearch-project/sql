/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.junit.Test;

public class RelevanceFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void test_wildcard_simple_query_string() throws IOException {
    String query1 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['Tags'], 'taste') | fields Id";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['T*'], 'taste') | fields Id";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));
  }

  /*
  Dash/minus ('-') character is interpreted as NOT flag if it is activated by NOT or ALL `flags` value
  `query1` searches for entries with '-free' in `Body`, `query2` - for entries without 'free' in `Body`
   */
  @Test
  public void verify_flags_in_simple_query_string() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE simple_query_string(['Body'], '-free', flags='NONE|PREFIX|ESCAPE')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE simple_query_string([Body], '-free', flags='NOT|AND|OR')";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query = "SOURCE=" + TEST_INDEX_BEER;
    var result = executeQuery(query);
    assertEquals(result2.getInt("total") + result1.getInt("total"), result.getInt("total"));
  }

  /*
  `escape` parameter switches regex-specific character escaping.
  `query1` searches for entries with "\\?" in `Title`, `query2` - for "?"
  Ref: QueryParserBase::escape in lucene code.
   */
  @Test
  public void verify_escape_in_query_string() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE query_string([Title], '?', escape=true);";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE query_string([Title], '?', escape=false);";
    var result2 = executeQuery(query2);
    assertEquals(0, result1.getInt("total"));
    assertEquals(8, result2.getInt("total"));
  }

  /*
  `default_operator`/`operator` in relevance search functions defines whether to search for all or for any words given.
  `query1` returns matches with 'beer' and matches with 'taste',
  `query2` returns matches with 'beer' and with 'taste' together.
   */
  @Test
  public void verify_default_operator_in_query_string() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE query_string([Title], 'beer taste', default_operator='OR')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE query_string([Title], 'beer taste', default_operator='AND')";
    var result2 = executeQuery(query2);
    assertEquals(16, result1.getInt("total"));
    assertEquals(4, result2.getInt("total"));
  }

  @Test
  public void verify_default_operator_in_simple_query_string() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE simple_query_string([Title], 'beer taste', default_operator='OR')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE simple_query_string([Title], 'beer taste', default_operator='AND')";
    var result2 = executeQuery(query2);
    assertEquals(16, result1.getInt("total"));
    assertEquals(4, result2.getInt("total"));
  }

  @Test
  public void verify_default_operator_in_multi_match() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE multi_match([Title], 'beer taste', operator='OR')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE multi_match([Title], 'beer taste', operator='AND')";
    var result2 = executeQuery(query2);
    assertEquals(16, result1.getInt("total"));
    assertEquals(4, result2.getInt("total"));
  }

  @Test
  public void verify_operator_in_match() throws IOException {
    String query1 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE match(Title, 'beer taste', operator='OR')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE="
        + TEST_INDEX_BEER + " | WHERE match(Title, 'beer taste', operator='AND')";
    var result2 = executeQuery(query2);
    assertEquals(16, result1.getInt("total"));
    assertEquals(4, result2.getInt("total"));
  }
}
