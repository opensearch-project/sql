/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.ParseCommandIT;

public class CalciteParseCommandIT extends ParseCommandIT {
  private static final String SUGGESTION_MATCHING_CONTENT = "capture groups must be alphanumeric";
  private static final String REDOS_INDEX = "redos_parse_guard";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createReDoSIndex();
  }

  /** A single document with a long text value so a catastrophic pattern actually blows up. */
  private void createReDoSIndex() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + REDOS_INDEX));
    } catch (ResponseException e) {
      // index doesn't exist yet, fine
    }
    Request createIndex = new Request("PUT", "/" + REDOS_INDEX);
    createIndex.setJsonEntity(
        "{\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {\"msg\": {\"type\": \"text\"}}}}");
    client().performRequest(createIndex);
    String longValue = "a".repeat(60);
    Request bulk = new Request("POST", "/" + REDOS_INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity("{\"index\":{\"_id\":\"1\"}}\n" + "{\"msg\": \"" + longValue + "\"}\n");
    client().performRequest(bulk);
  }

  /**
   * A pathological parse pattern must be rejected quickly by the backtracking guard instead of
   * pinning a worker thread. The timeout fails the test if the bound ever regresses.
   */
  @Test(timeout = 30000)
  public void testParseCatastrophicPatternIsRejectedQuickly() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | parse msg '(?<g>(.*a){8})b' | fields g", REDOS_INDEX));
      fail("Expected the catastrophic regex to be rejected by the backtracking guard");
    } catch (Exception e) {
      assertTrue(
          "Expected a ReDoS-guard error, got: " + e.getMessage(),
          e.getMessage().contains("too many characters"));
    }
  }

  /** The guard must be transparent: a legitimate parse still works. */
  @Test
  public void testParseLegitimatePatternStillWorks() throws IOException {
    org.json.JSONObject result =
        executeQuery(String.format("source=%s | parse msg '(?<g>a+)' | fields g", REDOS_INDEX));
    assertEquals(1, result.getJSONArray("datarows").length());
    assertEquals("a".repeat(60), result.getJSONArray("datarows").getJSONArray(0).get(0));
  }

  @Test
  public void testParseErrorInvalidGroupNameUnderscore() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host_name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for underscore in named capture group");
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host_name'"));
      assertTrue(e.getMessage().contains(SUGGESTION_MATCHING_CONTENT));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameHyphen() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host-name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for hyphen in named capture group");
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host-name'"));
      assertTrue(e.getMessage().contains(SUGGESTION_MATCHING_CONTENT));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameStartingWithDigit() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<1host>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for group name starting with digit");
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("Invalid capture group name '1host'"));
      assertTrue(e.getMessage().contains(SUGGESTION_MATCHING_CONTENT));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameSpecialCharacter() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host@name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for special character in named capture group");
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host@name'"));
      assertTrue(e.getMessage().contains(SUGGESTION_MATCHING_CONTENT));
    }
  }
}
