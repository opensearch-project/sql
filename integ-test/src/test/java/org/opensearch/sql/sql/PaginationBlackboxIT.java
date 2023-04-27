/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
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
public class PaginationBlackboxIT extends SQLIntegTestCase {

  private final String index;
  private final Integer pageSize;

  public PaginationBlackboxIT(@Name("index") String index,
                              @Name("pageSize") Integer pageSize) {
    this.index = index;
    this.pageSize = pageSize;
  }

  @ParametersFactory(argumentFormatting = "index = %1$s, page_size = %2$d")
  public static Iterable<Object[]> compareTwoDates() {
    var indices = new PaginationBlackboxHelper().getIndices();
    var pageSizes = List.of(5, 10, 100, 1000);
    var testData = new ArrayList<Object[]>();
    for (var index : indices) {
      for (var pageSize : pageSizes) {
        testData.add(new Object[] { index, pageSize });
      }
    }
    return testData;
  }

  @Test
  @SneakyThrows
  public void test_pagination_blackbox() {
    var response = executeJdbcRequest(String.format("select * from %s", index));
    var indexSize = response.getInt("total");
    var rows = response.getJSONArray("datarows");
    var schema = response.getJSONArray("schema");
    var testReportPrefix = String.format("index: %s, page size: %d || ", index, pageSize);
    var rowsPaged = new JSONArray();
    var rowsReturned = 0;
    response = new JSONObject(executeFetchQuery(
        String.format("select * from %s", index), pageSize, "jdbc"));
    var responseCounter = 1;
    this.logger.info(testReportPrefix + "first response");
    while (response.has("cursor")) {
      assertEquals(indexSize, response.getInt("total"));
      assertTrue("Paged response schema doesn't match to non-paged",
          schema.similar(response.getJSONArray("schema")));
      var cursor = response.getString("cursor");
      assertTrue(testReportPrefix + "Cursor returned from legacy engine",
          cursor.startsWith("n:"));
      rowsReturned += response.getInt("size");
      var datarows = response.getJSONArray("datarows");
      for (int i = 0; i < datarows.length(); i++) {
        rowsPaged.put(datarows.get(i));
      }
      response = executeCursorQuery(cursor);
      this.logger.info(testReportPrefix
          + String.format("subsequent response %d/%d", responseCounter++, (indexSize / pageSize) + 1));
    }
    assertTrue("Paged response schema doesn't match to non-paged",
        schema.similar(response.getJSONArray("schema")));
    assertEquals(0, response.getInt("total"));

    assertEquals(testReportPrefix + "Last page is not empty",
        0, response.getInt("size"));
    assertEquals(testReportPrefix + "Last page is not empty",
        0, response.getJSONArray("datarows").length());
    assertEquals(testReportPrefix + "Paged responses return another row count that non-paged",
        indexSize, rowsReturned);
    assertTrue(testReportPrefix + "Paged accumulated result has other rows than non-paged",
        rows.similar(rowsPaged));
  }

  // A dummy class created, because accessing to `client()` isn't available from a static context,
  // but it is needed before an instance of `PaginationBlackboxIT` is created.
  private static class PaginationBlackboxHelper extends SQLIntegTestCase {

    @SneakyThrows
    private List<String> getIndices() {
      initClient();
      loadIndex(Index.ACCOUNT);
      loadIndex(Index.BEER);
      loadIndex(Index.BANK);
      if (!isIndexExist(client(), "empty")) {
        executeRequest(new Request("PUT", "/empty"));
      }
      return Arrays.stream(getResponseBody(client().performRequest(new Request("GET", "_cat/indices?h=i")), true).split("\n"))
          // exclude this index, because it is too big and extends test time too long (almost 10k docs)
          .map(String::trim).filter(i -> !i.equals(TEST_INDEX_ONLINE)).collect(Collectors.toList());
    }
  }
}
