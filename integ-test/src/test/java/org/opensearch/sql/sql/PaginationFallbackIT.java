/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.util.TestUtils.verifyIsV1Cursor;
import static org.opensearch.sql.util.TestUtils.verifyIsV2Cursor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class PaginationFallbackIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
    loadIndex(Index.ONLINE);
    loadIndex(Index.BANK);
    updateClusterSettings(
        new ClusterSetting(
            PERSISTENT, Settings.Key.IGNORE_UNSUPPORTED_PAGINATION.getKeyValue(), "false"));
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
    var response =
        executeQueryTemplate("SELECT * FROM %s WHERE `11` = match_phrase('96')", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectWithHighlight() throws IOException {
    var response =
        executeQueryTemplate(
            "SELECT highlight(`11`) FROM %s WHERE match_query(`11`, '96')", TEST_INDEX_ONLINE);

    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectWithFullTextSearch() throws IOException {
    var response =
        executeQueryTemplate("SELECT * FROM %s WHERE match_phrase(`11`, '96')", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectFromIndexWildcard() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s*", TEST_INDEX);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectFromDataSource() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM @opensearch.%s", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSelectColumnReference() throws IOException {
    var response = executeQueryTemplate("SELECT `107` from %s", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testSubquery() throws IOException {
    var response = executeQueryTemplate("SELECT `107` from (SELECT * FROM %s)", TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testSelectExpression() throws IOException {
    var response = executeQueryTemplate("SELECT 1 + 1 - `107` from %s", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  public void testGroupBy() throws IOException {
    // GROUP BY is not paged by either engine.
    var response = executeQueryTemplate("SELECT * FROM %s GROUP BY `107`", TEST_INDEX_ONLINE);
    TestUtils.verifyNoCursor(response);
  }

  @Test
  public void testGroupByHaving() throws IOException {
    // GROUP BY is not paged by either engine.
    var response =
        executeQueryTemplate(
            "SELECT * FROM %s GROUP BY `107` HAVING `107` > 400", TEST_INDEX_ONLINE);
    TestUtils.verifyNoCursor(response);
  }

  @Test
  public void testLimit() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s LIMIT 8", TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testLimitOffset() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s LIMIT 8 OFFSET 4", TEST_INDEX_ONLINE);
    verifyIsV1Cursor(response);
  }

  @Test
  public void testOrderBy() throws IOException {
    var response = executeQueryTemplate("SELECT * FROM %s ORDER By `107`", TEST_INDEX_ONLINE);
    verifyIsV2Cursor(response);
  }

  @Test
  @SneakyThrows
  public void testFallbackSwitch() {
    var log =
        Paths.get(
            System.getProperty("project.root"),
            "build",
            "testclusters",
            "integTest-0",
            "logs",
            "integTest.log");
    // By default, the switch set to true - don't paginate if unsupported
    // Unset custom setting and check the default value
    updateClusterSettings(
        new ClusterSetting(
            PERSISTENT, Settings.Key.IGNORE_UNSUPPORTED_PAGINATION.getKeyValue(), null));
    var clusterSettings = getAllClusterSettings();
    assertEquals(
        "true",
        clusterSettings
            .getJSONObject("defaults")
            .getString(Settings.Key.IGNORE_UNSUPPORTED_PAGINATION.getKeyValue()));

    var logSize = Files.readAllLines(log).size();
    // Query should be executed on V2 (aggregation is not supported with paging there) without
    // cursor
    var response =
        executeQueryTemplate(
            "SELECT state, count(*) as `count` FROM %s GROUP BY state", TEST_INDEX_BANK);
    assertFalse(response.has("cursor"));
    assertEquals(7, response.getInt("total"));
    // Check logs
    var lines = Files.readAllLines(log);
    assertTrue(
        lines.stream()
            .skip(logSize)
            .anyMatch(l -> l.endsWith("Query executed without pagination.")));
    assertFalse(
        lines.stream()
            .skip(logSize)
            .anyMatch(
                l -> l.endsWith("Request is not supported and falling back to old SQL engine")));
    logSize = lines.size();

    updateClusterSettings(
        new ClusterSetting(
            PERSISTENT, Settings.Key.IGNORE_UNSUPPORTED_PAGINATION.getKeyValue(), "false"));

    // V1 fails to execute such query
    response =
        executeQueryTemplate(
            "SELECT state, count(*) as `count` FROM %s GROUP BY state", TEST_INDEX_BANK);
    assertEquals(
        response.getJSONObject("error").getString("reason"),
        "There was internal problem at backend");
    // Check logs
    lines = Files.readAllLines(log);
    assertFalse(
        lines.stream()
            .skip(logSize)
            .anyMatch(l -> l.endsWith("Query executed without pagination.")));
    assertTrue(
        lines.stream()
            .skip(logSize)
            .anyMatch(
                l -> l.endsWith("Request is not supported and falling back to old SQL engine")));
  }
}
