/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

// This class has only one test case, because it is parametrized and takes significant time
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginationBlackboxIT extends SQLIntegTestCase {

  private final Index index;
  private final Integer pageSize;

  public PaginationBlackboxIT(@Name("index") Index index,
                              @Name("pageSize") Integer pageSize) {
    this.index = index;
    this.pageSize = pageSize;
  }

  @Override
  protected void init() throws IOException {
    loadIndex(index);
  }

  @ParametersFactory(argumentFormatting = "index = %1$s, page_size = %2$d")
  public static Iterable<Object[]> compareTwoDates() {
    var indices = List.of(Index.ACCOUNT, Index.BEER, Index.BANK);
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
    var response = executeJdbcRequest(String.format("select * from %s", index.getName()));
    var indexSize = response.getInt("total");
    var rows = response.getJSONArray("datarows");
    var schema = response.getJSONArray("schema");
    var testReportPrefix = String.format("index: %s, page size: %d || ", index.getName(), pageSize);
    var rowsPaged = new JSONArray();
    var rowsReturned = 0;

    var responseCounter = 1;
    this.logger.info(testReportPrefix + "first response");
    response = new JSONObject(executeFetchQuery(
      String.format("select * from %s", index.getName()), pageSize, "jdbc"));

    var cursor = response.has("cursor")? response.getString("cursor") : "";
    do {
      this.logger.info(testReportPrefix
        + String.format("subsequent response %d/%d", responseCounter++, (indexSize / pageSize) + 1));
      assertTrue("Paged response schema doesn't match to non-paged",
          schema.similar(response.getJSONArray("schema")));

      rowsReturned += response.getInt("size");
      var datarows = response.getJSONArray("datarows");
      for (int i = 0; i < datarows.length(); i++) {
        rowsPaged.put(datarows.get(i));
      }

      if (response.has("cursor")) {
        TestUtils.verifyIsV2Cursor(response);
        cursor = response.getString("cursor");
        response = executeCursorQuery(cursor);
      } else {
        cursor = "";
      }

    } while(!cursor.isEmpty());
    assertTrue("Paged response schema doesn't match to non-paged",
        schema.similar(response.getJSONArray("schema")));

    assertEquals(testReportPrefix + "Paged responses return another row count that non-paged",
        indexSize, rowsReturned);
    assertTrue(testReportPrefix + "Paged accumulated result has other rows than non-paged",
        rows.similar(rowsPaged));
  }
}
