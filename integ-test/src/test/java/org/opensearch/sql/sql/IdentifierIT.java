/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createHiddenIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** Integration tests for identifiers including index and field name symbol. */
public class IdentifierIT extends SQLIntegTestCase {

  @Test
  public void testIndexNames() throws IOException {
    createIndexWithOneDoc("logs", "logs_2020_01");
    queryAndAssertTheDoc("SELECT * FROM logs");
    queryAndAssertTheDoc("SELECT * FROM logs_2020_01");
  }

  @Test
  public void testSpecialIndexNames() throws IOException {
    createIndexWithOneDoc(".system", "logs-2020-01");
    queryAndAssertTheDoc("SELECT * FROM .system");
    queryAndAssertTheDoc("SELECT * FROM logs-2020-01");
  }

  @Test
  public void testQuotedIndexNames() throws IOException {
    createIndexWithOneDoc("logs+2020+01", "logs.2020.01");
    queryAndAssertTheDoc("SELECT * FROM `logs+2020+01`");
  }

  @Test
  public void testSpecialFieldName() throws IOException {
    new Index("test").addDoc("{\"@timestamp\": 10, \"dimensions:major_version\": 30}");
    final JSONObject result =
        new JSONObject(
            executeQuery("SELECT @timestamp, " + "`dimensions:major_version` FROM test", "jdbc"));

    verifySchema(
        result,
        schema("@timestamp", null, "long"),
        schema("dimensions:major_version", null, "long"));
    verifyDataRows(result, rows(10, 30));
  }

  @Test
  public void testMultipleQueriesWithSpecialIndexNames() throws IOException {
    createIndexWithOneDoc("test.one", "test.two");
    queryAndAssertTheDoc("SELECT * FROM test.one");
    queryAndAssertTheDoc("SELECT * FROM test.two");
  }

  @Test
  public void testDoubleUnderscoreIdentifierTest() throws IOException {
    new Index("test.twounderscores").addDoc("{\"__age\": 30}");
    final JSONObject result =
        new JSONObject(executeQuery("SELECT __age FROM test.twounderscores", "jdbc"));

    verifySchema(result, schema("__age", null, "long"));
    verifyDataRows(result, rows(30));
  }

  @Test
  public void testMetafieldIdentifierTest() throws IOException {
    // create an index, but the contents doesn't matter
    String id = "12345";
    String index = "test.metafields";
    new Index(index).addDoc("{\"age\": 30}", id);

    // Execute using field metadata values
    final JSONObject result =
        new JSONObject(
            executeQuery(
                "SELECT *, _id, _index, _score, _maxscore, _sort " + "FROM " + index, "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(
        result,
        schema("age", null, "long"),
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_score", null, "float"),
        schema("_maxscore", null, "float"),
        schema("_sort", null, "long"));
    verifyDataRows(result, rows(30, id, index, 1.0, 1.0, -2));
  }

  @Test
  public void testMetafieldIdentifierRoutingSelectTest() throws IOException {
    // create an index, but the contents doesn't really matter
    String index = "test.routing_select";
    String mapping = "{\"_routing\": {\"required\": true }}";
    new Index(index, mapping)
        .addDocWithShardId("{\"age\": 31}", "test0", "test0")
        .addDocWithShardId("{\"age\": 31}", "test1", "test1")
        .addDocWithShardId("{\"age\": 32}", "test2", "test2")
        .addDocWithShardId("{\"age\": 33}", "test3", "test3")
        .addDocWithShardId("{\"age\": 34}", "test4", "test4")
        .addDocWithShardId("{\"age\": 35}", "test5", "test5");

    // Execute using field metadata values filtering on the routing shard hash id
    final JSONObject result =
        new JSONObject(
            executeQuery("SELECT age, _id, _index, _routing " + "FROM " + index, "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(
        result,
        schema("age", null, "long"),
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_routing", null, "keyword"));
    assertTrue(result.getJSONArray("schema").length() == 4);

    var datarows = result.getJSONArray("datarows");
    assertEquals(6, datarows.length());

    // note that _routing in the SELECT clause returns the shard
    for (int i = 0; i < 6; i++) {
      assertEquals("test" + i, datarows.getJSONArray(i).getString(1));
      assertEquals(index, datarows.getJSONArray(i).getString(2));
      assertTrue(datarows.getJSONArray(i).getString(3).contains("[" + index + "]"));
    }
  }

  @Test
  public void testMetafieldIdentifierRoutingFilterTest() throws IOException {
    // create an index, but the contents doesn't really matter
    String index = "test.routing_filter";
    String mapping = "{\"_routing\": {\"required\": true }}";
    new Index(index, mapping)
        .addDocWithShardId("{\"age\": 31}", "test1", "test1")
        .addDocWithShardId("{\"age\": 32}", "test2", "test2")
        .addDocWithShardId("{\"age\": 33}", "test3", "test3")
        .addDocWithShardId("{\"age\": 34}", "test4", "test4")
        .addDocWithShardId("{\"age\": 35}", "test5", "test5")
        .addDocWithShardId("{\"age\": 36}", "test6", "test6");

    // Execute using field metadata values filtering on the routing shard hash id
    final JSONObject result =
        new JSONObject(
            executeQuery(
                "SELECT _id, _index, _routing "
                    + "FROM "
                    + index
                    + " "
                    + "WHERE _routing = \\\"test4\\\"",
                "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(
        result,
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_routing", null, "keyword"));
    assertTrue(result.getJSONArray("schema").length() == 3);

    var datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    assertEquals("test4", datarows.getJSONArray(0).getString(0));
    // note that _routing in the SELECT clause returns the shard, not the routing hash id
    assertTrue(datarows.getJSONArray(0).getString(2).contains("[" + index + "]"));
  }

  @Test
  public void testMetafieldIdentifierWithAliasTest() throws IOException {
    // create an index, but the contents doesn't matter
    String id = "99999";
    String index = "test.aliasmetafields";
    new Index(index).addDoc("{\"age\": 30}", id);

    // Execute using field metadata values
    final JSONObject result =
        new JSONObject(
            executeQuery(
                "SELECT _id AS A, _index AS B, _score AS C, _maxscore AS D, _sort AS E "
                    + "FROM "
                    + index
                    + " "
                    + "WHERE _id = \\\""
                    + id
                    + "\\\"",
                "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(
        result,
        schema("_id", "A", "keyword"),
        schema("_index", "B", "keyword"),
        schema("_score", "C", "float"),
        schema("_maxscore", "D", "float"),
        schema("_sort", "E", "long"));
    verifyDataRows(result, rows(id, index, null, null, -2));
  }

  private void createIndexWithOneDoc(String... indexNames) throws IOException {
    for (String indexName : indexNames) {
      new Index(indexName).addDoc("{\"age\": 30}");
    }
  }

  private void queryAndAssertTheDoc(String sql) {
    final JSONObject result = new JSONObject(executeQuery(sql.replace("\"", "\\\""), "jdbc"));
    verifySchema(result, schema("age", null, "long"));
    verifyDataRows(result, rows(30));
  }

  /** Index abstraction for test code readability. */
  private static class Index {

    private final String indexName;

    Index(String indexName) throws IOException {
      this.indexName = indexName;

      if (indexName.startsWith(".")) {
        createHiddenIndexByRestClient(client(), indexName, "");
      } else {
        executeRequest(new Request("PUT", "/" + indexName));
      }
    }

    Index(String indexName, String mapping) throws IOException {
      this.indexName = indexName;

      Request createIndex = new Request("PUT", "/" + indexName);
      createIndex.setJsonEntity(mapping);
      executeRequest(new Request("PUT", "/" + indexName));
    }

    void addDoc(String doc) {
      Request indexDoc = new Request("POST", String.format("/%s/_doc?refresh=true", indexName));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
    }

    public Index addDoc(String doc, String id) {
      Request indexDoc =
          new Request("POST", String.format("/%s/_doc/%s?refresh=true", indexName, id));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
      return this;
    }

    public Index addDocWithShardId(String doc, String id, String routing) {
      Request indexDoc =
          new Request(
              "POST", String.format("/%s/_doc/%s?refresh=true&routing=%s", indexName, id, routing));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
      return this;
    }
  }
}
