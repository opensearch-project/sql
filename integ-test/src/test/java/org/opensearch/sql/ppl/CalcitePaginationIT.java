/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/** Integration tests for PPL pagination with Calcite execution engine. */
public class CalcitePaginationIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    enableCalcite();
  }

  @Test
  public void testBasicPaginationWithAccount() throws IOException {
    // Page 1: offset=0, pageSize=3, sorted by age
    JSONObject page1 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, lastname, age | sort age | head 1000",
                TEST_INDEX_ACCOUNT),
            3,
            0);

    verifySchema(
        page1,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
    JSONArray datarows1 = page1.getJSONArray("datarows");
    assertEquals("Page 1 should have 3 rows", 3, datarows1.length());
    // All ages on page 1 should be 20 (youngest)
    for (int i = 0; i < datarows1.length(); i++) {
      int age = datarows1.getJSONArray(i).getInt(2);
      assertEquals("First page should have age 20", 20, age);
    }

    // Page 2: offset=3, pageSize=3
    JSONObject page2 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, lastname, age | sort age | head 1000",
                TEST_INDEX_ACCOUNT),
            3,
            3);

    verifySchema(
        page2,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
    // Verify we get different rows on page 2 (all should have age >= 20)
    JSONArray datarows2 = page2.getJSONArray("datarows");
    assertEquals("Page 2 should have 3 rows", 3, datarows2.length());
    for (int i = 0; i < datarows2.length(); i++) {
      int age = datarows2.getJSONArray(i).getInt(2);
      assertTrue("Age should be >= 20", age >= 20);
    }
  }

  @Test
  public void testPaginationWithFilter() throws IOException {
    // Filter should be applied before pagination - get accounts with age > 35
    JSONObject page1 =
        executePaginatedQuery(
            String.format(
                "source=%s | where age > 35 | fields firstname, age | sort age",
                TEST_INDEX_ACCOUNT),
            5,
            0);

    verifySchema(page1, schema("firstname", "string"), schema("age", "bigint"));

    JSONArray datarows = page1.getJSONArray("datarows");
    assertTrue("Should return data", datarows.length() > 0);
    assertTrue("Should return at most 5 rows", datarows.length() <= 5);

    // Verify all ages are > 35
    for (int i = 0; i < datarows.length(); i++) {
      int age = datarows.getJSONArray(i).getInt(1);
      assertTrue("Age should be > 35", age > 35);
    }
  }

  @Test
  public void testPaginationWithOrderBy() throws IOException {
    // Page 1 with order by age ascending
    JSONObject page1 =
        executePaginatedQuery(
            String.format("source=%s | fields age | sort age | head 1000", TEST_INDEX_ACCOUNT),
            3,
            0);

    verifySchema(page1, schema("age", "bigint"));
    JSONArray datarows1 = page1.getJSONArray("datarows");
    assertEquals(3, datarows1.length());

    // Page 2
    JSONObject page2 =
        executePaginatedQuery(
            String.format("source=%s | fields age | sort age | head 1000", TEST_INDEX_ACCOUNT),
            3,
            3);

    verifySchema(page2, schema("age", "bigint"));
    JSONArray datarows2 = page2.getJSONArray("datarows");
    assertEquals(3, datarows2.length());

    // Last item from page 1 should be <= first item from page 2
    int lastAgePage1 = datarows1.getJSONArray(2).getInt(0);
    int firstAgePage2 = datarows2.getJSONArray(0).getInt(0);
    assertTrue("Results should maintain sort order across pages", lastAgePage1 <= firstAgePage2);
  }

  @Test
  public void testPaginationWithBankDataset() throws IOException {
    // Test with BANK dataset - page 1: offset=0, pageSize=3
    JSONObject page1 =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age", TEST_INDEX_BANK), 3, 0);

    verifySchema(page1, schema("firstname", "string"), schema("age", "int"));
    // BANK dataset has 7 records sorted by age: 28, 32, 33, 34, 36, 36, 39
    verifyDataRows(page1, rows("Nanette", 28), rows("Amber JOHnny", 32), rows("Dale", 33));

    // Page 2: offset=3, pageSize=3
    JSONObject page2 =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age", TEST_INDEX_BANK), 3, 3);

    verifySchema(page2, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(page2, rows("Dillard", 34), rows("Hattie", 36), rows("Elinor", 36));

    // Page 3: offset=6, pageSize=3 - should only get 1 remaining record
    JSONObject page3 =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age", TEST_INDEX_BANK), 3, 6);

    verifySchema(page3, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(page3, rows("Virginia", 39));
  }

  @Test
  public void testSinglePageResult() throws IOException {
    // Request more data than available (using filter to limit results)
    JSONObject result =
        executePaginatedQuery(
            String.format(
                "source=%s | where firstname='Amber' | fields firstname", TEST_INDEX_ACCOUNT),
            100,
            0);

    verifySchema(result, schema("firstname", "string"));
    // Amber Duke exists in the account dataset
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testOffsetBeyondData() throws IOException {
    // Offset beyond available data in BANK dataset (7 records)
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | fields firstname", TEST_INDEX_BANK), 10, 100);

    verifySchema(result, schema("firstname", "string"));
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals("Should return empty when offset is beyond data", 0, datarows.length());
  }

  @Test
  public void testZeroPageSize() throws IOException {
    // When pageSize is 0, it should fall back to system limit behavior
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | head 5", TEST_INDEX_ACCOUNT), 0, 0);

    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
    JSONArray datarows = result.getJSONArray("datarows");
    // Should return data with system default limit (head 5 limits to 5)
    assertEquals("Should return 5 rows with head limit", 5, datarows.length());
  }

  @Test
  public void testSchemaConsistencyAcrossPages() throws IOException {
    // Schema should be consistent across all pages
    JSONObject page1 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, age, gender | head 1000", TEST_INDEX_ACCOUNT),
            5,
            0);

    JSONObject page2 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, age, gender | head 1000", TEST_INDEX_ACCOUNT),
            5,
            5);

    // Both pages should have consistent schema
    verifySchema(
        page1, schema("firstname", "string"), schema("age", "bigint"), schema("gender", "string"));
    verifySchema(
        page2, schema("firstname", "string"), schema("age", "bigint"), schema("gender", "string"));

    JSONArray schema1 = page1.getJSONArray("schema");
    JSONArray schema2 = page2.getJSONArray("schema");
    assertEquals(
        "Schema should be consistent across pages", schema1.toString(), schema2.toString());
  }

  @Test
  public void testPaginationWithAggregation() throws IOException {
    // Aggregation results should also support pagination
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | stats count() by gender", TEST_INDEX_ACCOUNT), 10, 0);

    verifySchema(result, schema("count()", "bigint"), schema("gender", "string"));
    JSONArray datarows = result.getJSONArray("datarows");
    assertTrue("Aggregation should return results", datarows.length() > 0);
    // Should have M and F groups
    assertTrue("Should have at most 2 gender groups", datarows.length() <= 2);
  }

  @Test
  public void testPaginationIterateThroughAllPages() throws IOException {
    // Iterate through all pages to verify complete data coverage
    int pageSize = 3;
    int totalRecords = 0;
    int pageCount = 0;
    int maxPages = 10; // Safety limit

    while (pageCount < maxPages) {
      JSONObject page =
          executePaginatedQuery(
              String.format("source=%s | fields firstname, age | sort age", TEST_INDEX_BANK),
              pageSize,
              pageCount * pageSize);

      JSONArray datarows = page.getJSONArray("datarows");
      if (datarows.length() == 0) {
        break;
      }

      totalRecords += datarows.length();
      pageCount++;
    }

    // BANK dataset has 7 records
    assertEquals("Should retrieve all 7 records across pages", 7, totalRecords);
    assertEquals("Should take 3 pages to get all records", 3, pageCount);
  }

  @Test
  public void testPaginationWithHeadCommand() throws IOException {
    // User query limits to 6 rows with head, pagination takes first 3
    // Expected: 3 rows (pagination applied after head)
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age | head 6", TEST_INDEX_BANK),
            3, // pageSize
            0); // offset

    verifySchema(result, schema("firstname", "string"), schema("age", "int"));
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals("Should return 3 rows (pageSize)", 3, datarows.length());
    // First 3 of head 6 result
    verifyDataRows(result, rows("Nanette", 28), rows("Amber JOHnny", 32), rows("Dale", 33));
  }

  @Test
  public void testPaginationWithHeadFrom() throws IOException {
    // User query: head 4 from 2 (skip 2, take 4)
    // BANK data sorted by age: Nanette(28), Amber(32), Dale(33), Dillard(34), Hattie(36),
    // Elinor(36), Virginia(39)
    // After head 4 from 2: Dale(33), Dillard(34), Hattie(36), Elinor(36)
    // Pagination page 1 (pageSize=2, offset=0): Dale(33), Dillard(34)
    JSONObject page1 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, age | sort age | head 4 from 2", TEST_INDEX_BANK),
            2, // pageSize
            0); // offset

    verifySchema(page1, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(page1, rows("Dale", 33), rows("Dillard", 34));

    // Pagination page 2 (pageSize=2, offset=2): Hattie(36), Elinor(36)
    JSONObject page2 =
        executePaginatedQuery(
            String.format(
                "source=%s | fields firstname, age | sort age | head 4 from 2", TEST_INDEX_BANK),
            2, // pageSize
            2); // offset

    verifySchema(page2, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(page2, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testPaginationWithHeadSecondPage() throws IOException {
    // User query: head 6 (returns 6 rows from BANK dataset)
    // Pagination page 2: pageSize=3, offset=3
    // Expected: rows 4-6 of the head result
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age | head 6", TEST_INDEX_BANK),
            3, // pageSize
            3); // offset for second page

    verifySchema(result, schema("firstname", "string"), schema("age", "int"));
    // Second 3 rows of head 6: Dillard(34), Hattie(36), Elinor(36)
    verifyDataRows(result, rows("Dillard", 34), rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testPaginationPageSizeLargerThanHead() throws IOException {
    // User query: head 3 (returns 3 rows)
    // Pagination pageSize=10 is larger than head result
    // Expected: only 3 rows (limited by head)
    JSONObject result =
        executePaginatedQuery(
            String.format("source=%s | fields firstname, age | sort age | head 3", TEST_INDEX_BANK),
            10, // pageSize larger than head
            0);

    verifySchema(result, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(result, rows("Nanette", 28), rows("Amber JOHnny", 32), rows("Dale", 33));
  }

  /**
   * Execute a PPL query with pagination parameters.
   *
   * @param query the PPL query
   * @param pageSize the page size (0 = no pagination)
   * @param offset the offset (0-based)
   * @return the JSON response
   */
  protected JSONObject executePaginatedQuery(String query, int pageSize, int offset)
      throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);

    String jsonEntity;
    if (pageSize > 0) {
      jsonEntity =
          String.format(
              Locale.ROOT,
              "{\n  \"query\": \"%s\",\n  \"pageSize\": %d,\n  \"offset\": %d\n}",
              query,
              pageSize,
              offset);
    } else {
      jsonEntity = String.format(Locale.ROOT, "{\n  \"query\": \"%s\"\n}", query);
    }

    request.setJsonEntity(jsonEntity);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(getResponseBody(response, true));
  }
}
