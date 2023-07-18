/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.multiClusterSearch;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createHiddenIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ShardingSearchIT extends SQLIntegTestCase {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private final static String MAPPING = "{ " +
      "\"settings\": {" +
      "  \"number_of_shards\": 3" +
      "}," +
      "\"mappings\" : {" +
      "  \"_routing\": { \"required\": true }," +
      "  \"properties\" : { " +
      "    \"age\" : { \"type\" : \"long\" } } } }";

  @Override
  public void init() throws IOException {
    configureMultiClusters(MULTI_REMOTE_CLUSTER);
  }

  @Test
  public void testMetafieldIdentifierRoutingTest() throws IOException {
    // create an index, but the contents doesn't really matter
    String index = "test.routing_partition";
    new Index(index, MAPPING)
        .addDocWithShardId("{\"age\": 31}", "test0", "test0")
        .addDocWithShardId("{\"age\": 31}", "test1", "test1")
        .addDocWithShardId("{\"age\": 32}", "test2", "test2")
        .addDocWithShardId("{\"age\": 33}", "test3", "test3")
        .addDocWithShardId("{\"age\": 34}", "test4", "test4")
        .addDocWithShardId("{\"age\": 35}", "test5", "test5");

    // Execute using field metadata values filtering on the routing shard hash id
    String query = "SELECT age, _id, _index, _routing "
        + "FROM " + index + " partition(test4)";
    final JSONObject result = new JSONObject(executeQuery(query, "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(result,
        schema("age", null, "long"),
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_routing", null, "keyword"));
    assertTrue(result.getJSONArray("schema").length() == 4);

    // expect AT LEAST one result as we're requested all data on a single shard,
    // but multiple _routing hashes can point to the same shard
    var datarows = result.getJSONArray("datarows");
    assertTrue(datarows.length() > 0);
  }

  @Test
  public void testMetafieldIdentifierRoutingWhereTest() throws IOException {
    // create an index, but the contents doesn't really matter
    String index = "test.routing_where";
    new Index(index, MAPPING)
        .addDocWithShardId("{\"age\": 31}", "test0", "test0")
        .addDocWithShardId("{\"age\": 31}", "test1", "test1")
        .addDocWithShardId("{\"age\": 32}", "test2", "test2")
        .addDocWithShardId("{\"age\": 33}", "test3", "test3")
        .addDocWithShardId("{\"age\": 34}", "test4", "test4")
        .addDocWithShardId("{\"age\": 35}", "test5", "test5");

    // Execute using field metadata values filtering on the routing shard hash id
    String query = "SELECT age, _id, _index, _routing "
        + "FROM " + index + " partition(test4) "
        + "WHERE _routing='test4'";
    final JSONObject result = new JSONObject(executeQuery(query, "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(result,
        schema("age", null, "long"),
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_routing", null, "keyword"));
    assertTrue(result.getJSONArray("schema").length() == 4);

    // expect exactly one result as we're filtering on the _routing shard
    var datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    assertEquals("test4", datarows.getJSONArray(0).getString(1));
    assertEquals(index, datarows.getJSONArray(0).getString(2));
    assertEquals("test4", datarows.getJSONArray(0).getString(3));
  }

  @Test
  public void testMetafieldIdentifierRoutingWithoutPartitionTest() throws IOException {
    // create an index with 3 shards
    String index = "test.routing_empty";
    new Index(index, MAPPING)
        .addDocWithShardId("{\"age\": 31}", "test0", "test0")
        .addDocWithShardId("{\"age\": 31}", "test1", "test1")
        .addDocWithShardId("{\"age\": 32}", "test2", "test2")
        .addDocWithShardId("{\"age\": 33}", "test3", "test3")
        .addDocWithShardId("{\"age\": 34}", "test4", "test4")
        .addDocWithShardId("{\"age\": 35}", "test5", "test5");

    // Execute using field metadata values filtering on the routing shard hash id
    String query = "SELECT age, _id, _index, _routing "
        + "FROM " + index;
    final JSONObject result = new JSONObject(executeQuery(query, "jdbc"));

    // Verify that the metadata values are returned when requested
    verifySchema(result,
        schema("age", null, "long"),
        schema("_id", null, "keyword"),
        schema("_index", null, "keyword"),
        schema("_routing", null, "keyword"));
    assertTrue(result.getJSONArray("schema").length() == 4);

    // expect that when partition/shard is not specified, all data is returned
    var datarows = result.getJSONArray("datarows");
    assertEquals(6, datarows.length());
  }

  @Test
  public void testWithoutRoutingThrowsTest() throws IOException {
    Index index = new Index("test.routing_error", MAPPING);

    // routing is required when pushing data
    final IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> index.addDoc("{\"age\": 31}", "test0"));
  }

  /**
   * Index abstraction for test code readability.
   */
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
      executeRequest(createIndex);
    }

    void addDoc(String doc) {
      Request indexDoc = new Request("POST", String.format("/%s/_doc?refresh=true", indexName));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
    }

    public Index addDoc(String doc, String id) {
      Request indexDoc = new Request("POST", String.format("/%s/_doc/%s?refresh=true", indexName, id));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
      return this;
    }

    public Index addDocWithShardId(String doc, String id, String routing) {
      Request indexDoc = new Request("POST", String.format("/%s/_doc/%s?refresh=true&routing=%s", indexName, id, routing));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
      return this;
    }
  }
}
