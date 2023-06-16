/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Test pagination with `WHERE` clause using a parametrized test.
 * See constructor {@link #PaginationFilterIT} for list of parameters
 * and {@link #generateParameters} and {@link #STATEMENT_TO_NUM_OF_PAGES}
 * to see how these parameters are generated.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginationFilterIT extends SQLIntegTestCase {

  /**
   * Map of the OS-SQL statement sent to SQL-plugin, and the total number
   * of expected hits (on all pages) from the filtered result
   */
  final private static Map<String, Integer> STATEMENT_TO_NUM_OF_PAGES = Map.of(
      "SELECT * FROM " + TestsConstants.TEST_INDEX_ACCOUNT, 1000,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " WHERE match(address, 'street')", 385,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " WHERE match(address, 'street') AND match(city, 'Ola')", 1,
      "SELECT firstname, lastname, highlight(address) FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " WHERE match(address, 'street') AND match(state, 'OH')", 5,
      "SELECT firstname, lastname, highlight('*') FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " WHERE match(address, 'street') AND match(state, 'OH')", 5,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_BEER + " WHERE true", 60,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_BEER + " WHERE Id=10", 1,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_BEER + " WHERE Id + 5=15", 1,
      "SELECT * FROM " + TestsConstants.TEST_INDEX_BANK, 7
  );

  private final String sqlStatement;

  private final Integer totalHits;
  private final Integer pageSize;

  public PaginationFilterIT(@Name("statement") String sqlStatement,
                            @Name("total_hits") Integer totalHits,
                            @Name("page_size") Integer pageSize) {
    this.sqlStatement = sqlStatement;
    this.totalHits = totalHits;
    this.pageSize = pageSize;
  }

  @Override
  public void init() throws IOException {
    initClient();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BEER);
    loadIndex(Index.BANK);
  }

  @ParametersFactory(argumentFormatting = "query = %1$s, total_hits = %2$d, page_size = %3$d")
  public static Iterable<Object[]> generateParameters() {
    List<Integer> pageSizes = List.of(5, 1000);
    List<Object[]> testData = new ArrayList<Object[]>();

    STATEMENT_TO_NUM_OF_PAGES.forEach((statement, totalHits) -> {
      for (var pageSize : pageSizes) {
        testData.add(new Object[] { statement, totalHits, pageSize });
      }
    });
    return testData;
  }

  /**
   * Test compares non-paginated results with paginated results
   * To ensure that the pushdowns return the same number of hits even
   * with filter WHERE pushed down
   */
  @Test
  @SneakyThrows
  public void test_pagination_with_where() {
    // get non-paginated result for comparison
    JSONObject nonPaginatedResponse = executeJdbcRequest(sqlStatement);
    int totalResultsCount = nonPaginatedResponse.getInt("total");
    JSONArray rows = nonPaginatedResponse.getJSONArray("datarows");
    JSONArray schema = nonPaginatedResponse.getJSONArray("schema");
    var testReportPrefix = String.format("query: %s; total hits: %d; page size: %d || ", sqlStatement, totalResultsCount, pageSize);
    assertEquals(totalHits.intValue(), totalResultsCount);

    var rowsPaged = new JSONArray();
    var pagedSize = 0;
    var responseCounter = 1;

    // make first request - with a cursor
    JSONObject paginatedResponse = new JSONObject(executeFetchQuery(sqlStatement, pageSize, "jdbc"));
    this.logger.info(testReportPrefix + "<first response>");
    do {
      var cursor = paginatedResponse.has("cursor") ? paginatedResponse.getString("cursor") : null;
      pagedSize += paginatedResponse.getInt("size");
      var datarows = paginatedResponse.getJSONArray("datarows");
      for (int i = 0; i < datarows.length(); i++) {
        rowsPaged.put(datarows.get(i));
      }

      assertTrue(
          "Paged response schema doesn't match to non-paged",
          schema.similar(paginatedResponse.getJSONArray("schema")));

      if (cursor != null) {
        assertTrue(
            testReportPrefix + "Cursor returned from legacy engine",
            cursor.startsWith("n:"));

        paginatedResponse = executeCursorQuery(cursor);

        this.logger.info(testReportPrefix
            + String.format("response %d/%d", responseCounter++, (totalResultsCount / pageSize) + 1));
      } else {
        break;
      }
    } while (true);
    // last page expected results:
    assertEquals(testReportPrefix + "Last page",
        totalHits % pageSize, paginatedResponse.getInt("size"));
    assertEquals(testReportPrefix + "Last page",
        totalHits % pageSize, paginatedResponse.getJSONArray("datarows").length());

    // compare paginated and non-paginated counts
    assertEquals(testReportPrefix + "Paged responses returned an unexpected total",
        totalResultsCount, pagedSize);
    assertEquals(testReportPrefix + "Paged responses returned an unexpected rows count",
        rows.length(), rowsPaged.length());
  }
}
