/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

// This class has only one test case, because it is parametrized and takes significant time
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginationFilterIT extends SQLIntegTestCase {

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

  @ParametersFactory(argumentFormatting = "select = %1$s, total_hits = %2$d, page_size = %3$d")
  public static Iterable<Object[]> compareTwoDates() {
    Map<String, Integer> statements = new PaginationBlackboxHelper().getStatements();

    List<Integer> pageSizes = List.of(5, 1000);
    List<Object[]> testData = new ArrayList<Object[]>();

    statements.forEach((statement, totalHits) -> {
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
  public void test_pagination_blackbox() {
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
    while (paginatedResponse.has("cursor")) {
      var cursor = paginatedResponse.getString("cursor");
      pagedSize += paginatedResponse.getInt("size");
      var datarows = paginatedResponse.getJSONArray("datarows");
      for (int i = 0; i < datarows.length(); i++) {
        rowsPaged.put(datarows.get(i));
      }

      assertEquals(totalResultsCount, paginatedResponse.getInt("total"));
      assertTrue(
          "Paged response schema doesn't match to non-paged",
          schema.similar(paginatedResponse.getJSONArray("schema")));
      assertTrue(
          testReportPrefix + "Cursor returned from legacy engine",
          cursor.startsWith("n:"));

      paginatedResponse = executeCursorQuery(cursor);
      this.logger.info(testReportPrefix
          + String.format("response %d/%d", responseCounter++, (totalResultsCount / pageSize) + 1));
    }
    // last page expected results:
    assertEquals(0, paginatedResponse.getInt("total"));
    assertEquals(testReportPrefix + "Last page is not empty",
        0, paginatedResponse.getInt("size"));
    assertEquals(testReportPrefix + "Last page is not empty",
        0, paginatedResponse.getJSONArray("datarows").length());

    // compare paginated and non-paginated counts
    assertEquals(testReportPrefix + "Paged responses returned an unexpected total",
        totalResultsCount, pagedSize);
    assertEquals(testReportPrefix + "Paged responses returned an unexpected rows count",
        rows.length(), rowsPaged.length());
  }

  // A dummy class created, because accessing to `client()` isn't available from a static context,
  // but it is needed before an instance of `PaginationBlackboxIT` is created.
  private static class PaginationBlackboxHelper extends SQLIntegTestCase {

    /**
     * Map of the OS-SQL statement sent to SQL-plugin, and the total number
     * of expected hits (on all pages) from the filtered result
     */
    final private static Map<String, Integer> STATEMENT_TO_NUM_OF_PAGES = Map.of(
        "SELECT * FROM opensearch-sql_test_index_account", 1000,
        "SELECT * FROM opensearch-sql_test_index_account WHERE match(address, 'street')", 385,
        "SELECT * FROM opensearch-sql_test_index_account WHERE match(address, 'street') AND match(city, 'Ola')", 1,
        "SELECT firstname, lastname, highlight(address) FROM opensearch-sql_test_index_account WHERE match(address, 'street') AND match(state, 'OH')", 5,
        "SELECT firstname, lastname, highlight('*') FROM opensearch-sql_test_index_account WHERE match(address, 'street') AND match(state, 'OH')", 5,
        "SELECT * FROM opensearch-sql_test_index_beer WHERE true", 60,
        "SELECT * FROM opensearch-sql_test_index_beer WHERE Id=10", 1,
        "SELECT * FROM opensearch-sql_test_index_beer WHERE Id + 5=15", 1,
        "SELECT * FROM opensearch-sql_test_index_bank", 7
    );

    private Map<String, Integer> getStatements() {
      return STATEMENT_TO_NUM_OF_PAGES;
    }
  }
}
