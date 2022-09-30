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

/**
 * Integration tests for identifiers including index and field name symbol.
 */
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
    new Index("test")
        .addDoc("{\"@timestamp\": 10, \"dimensions:major_version\": 30}");
    final JSONObject result = new JSONObject(executeQuery("SELECT @timestamp, "
        + "`dimensions:major_version` FROM test", "jdbc"));

    verifySchema(result,
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

    void addDoc(String doc) {
      Request indexDoc = new Request("POST", String.format("/%s/_doc?refresh=true", indexName));
      indexDoc.setJsonEntity(doc);
      performRequest(client(), indexDoc);
    }
  }

}
