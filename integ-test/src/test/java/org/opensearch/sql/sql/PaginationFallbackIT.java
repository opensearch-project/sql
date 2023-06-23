/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.util.TestUtils.verifyIsV1Cursor;
import static org.opensearch.sql.util.TestUtils.verifyIsV2Cursor;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class PaginationFallbackIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
    loadIndex(Index.ONLINE);
  }

  @Test
  public void testWhereClause() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s WHERE 1 = 1", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectAll() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectWithOpenSearchFuncInFilter() throws IOException {
    var response = executeQueryTemplate(
        "SELECT * FROM %s WHERE `11` = match_phrase('96')", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectWithHighlight() throws IOException {
    var response = executeQueryTemplate(
        "SELECT highlight(`11`) FROM %s WHERE match_query(`11`, '96')", TEST_INDEX_ONLINE);

    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectWithFullTextSearch() throws IOException {
    var response = executeQueryTemplate(
        "SELECT * FROM %s WHERE match_phrase(`11`, '96')", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectFromIndexWildcard() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s*", TEST_INDEX);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectFromDataSource() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM @opensearch.%s",
        TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectColumnReference() throws IOException {
    var response = executeQueryTemplate("SELECT `107` from %s", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSubquery() throws IOException {
    var response = executeQueryTemplate("SELECT `107` from (SELECT * FROM %s)",
        TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testSelectExpression() throws IOException {
    var response = executeQueryTemplate("SELECT 1 + 1 - `107` from %s",
        TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testGroupBy() throws IOException {
    // GROUP BY is not paged by either engine.
     var response = executeQueryTemplate("SELECT * FROM %s GROUP BY `107`",
      TEST_INDEX_ONLINE);
    TestUtils.verifyNoCursor(response);
  }

  @Test
  public void testGroupByHaving() throws IOException {
    // GROUP BY is not paged by either engine.
    var response = executeQueryTemplate("SELECT * FROM %s GROUP BY `107` HAVING `107` > 400",
        TEST_INDEX_ONLINE);
    TestUtils.verifyNoCursor(response);
  }

  @Test
  public void testLimit() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s LIMIT 8", TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testLimitOffset() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s LIMIT 8 OFFSET 4",
        TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testOrderBy() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s ORDER By `107`",
        TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }
}
