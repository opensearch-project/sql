/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_AIRPORTS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_EMPLOYEES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_TRAVELERS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for graphLookup command. Test cases are inspired by MongoDB's $graphLookup
 * examples.
 *
 * <p>Test data:
 *
 * <ul>
 *   <li>graph_employees: Employee hierarchy (Dev->Eliot->Ron->Andrew, Asya->Ron, Dan->Andrew)
 *   <li>graph_travelers: Social network with friends connections
 *   <li>graph_airports: Airport connections (JFK, BOS, ORD, PWM, LHR)
 * </ul>
 *
 * @see <a
 *     href="https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/">MongoDB
 *     $graphLookup</a>
 */
public class CalcitePPLGraphLookupIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.GRAPH_EMPLOYEES);
    loadIndex(Index.GRAPH_TRAVELERS);
    loadIndex(Index.GRAPH_AIRPORTS);
  }

  // ==================== Employee Hierarchy Tests ====================

  /** Test 1: Basic employee hierarchy traversal. Find all managers in the reporting chain. */
  @Test
  public void testEmployeeHierarchyBasicTraversal() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows("Dev", "Eliot", 1, List.of("{Eliot, Ron, 2}")),
        rows("Eliot", "Ron", 2, List.of("{Ron, Andrew, 3}")),
        rows("Ron", "Andrew", 3, List.of("{Andrew, null, 4}")),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows("Asya", "Ron", 5, List.of("{Ron, Andrew, 3}")),
        rows("Dan", "Andrew", 6, List.of("{Andrew, null, 4}")));
  }

  /** Test 2: Employee hierarchy traversal with depth field. */
  @Test
  public void testEmployeeHierarchyWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " depthField=level"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows("Dev", "Eliot", 1, List.of("{Eliot, Ron, 2, 0}")),
        rows("Eliot", "Ron", 2, List.of("{Ron, Andrew, 3, 0}")),
        rows("Ron", "Andrew", 3, List.of("{Andrew, null, 4, 0}")),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows("Asya", "Ron", 5, List.of("{Ron, Andrew, 3, 0}")),
        rows("Dan", "Andrew", 6, List.of("{Andrew, null, 4, 0}")));
  }

  /** Test 3: Employee hierarchy with maxDepth=1 (allows 2 levels of traversal). */
  @Test
  public void testEmployeeHierarchyWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " maxDepth=1"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows("Dev", "Eliot", 1, List.of("{Eliot, Ron, 2}", "{Ron, Andrew, 3}")),
        rows("Eliot", "Ron", 2, List.of("{Ron, Andrew, 3}", "{Andrew, null, 4}")),
        rows("Ron", "Andrew", 3, List.of("{Andrew, null, 4}")),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows("Asya", "Ron", 5, List.of("{Ron, Andrew, 3}", "{Andrew, null, 4}")),
        rows("Dan", "Andrew", 6, List.of("{Andrew, null, 4}")));
  }

  /** Test 4: Query Dev's complete reporting chain: Dev->Eliot->Ron->Andrew */
  @Test
  public void testEmployeeHierarchyForSpecificEmployee() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Dev'"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(result, rows("Dev", "Eliot", 1, List.of("{Eliot, Ron, 2}")));
  }

  // ==================== Social Network (Travelers) Tests ====================

  /**
   * Test 5: Find all friends (direct and indirect) for travelers. Note: Currently returns empty
   * socialNetwork arrays because the friends field is an array type, which the current
   * implementation doesn't fully traverse.
   */
  @Test
  public void testTravelersFriendsNetwork() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=friends"
                    + " fromField=friends"
                    + " toField=name"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("hobbies", "string"),
        schema("friends", "string"),
        schema("socialNetwork", "array"));
    verifyDataRows(
        result,
        rows(
            "Tanya Jordan",
            List.of("tennis", "reading"),
            List.of("Shirley Soto", "Terry Hawkins"),
            Collections.emptyList()),
        rows(
            "Shirley Soto",
            List.of("golf", "reading"),
            List.of("Tanya Jordan", "Terry Hawkins"),
            Collections.emptyList()),
        rows(
            "Terry Hawkins",
            List.of("tennis", "golf"),
            List.of("Tanya Jordan", "Shirley Soto"),
            Collections.emptyList()),
        rows("Brad Green", List.of("reading"), List.of("Shirley Soto"), Collections.emptyList()));
  }

  /** Test 6: Brad Green's friends network with maxDepth=1. */
  @Test
  public void testTravelersFriendsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Brad Green'"
                    + " | graphLookup %s"
                    + " startField=friends"
                    + " fromField=friends"
                    + " toField=name"
                    + " maxDepth=1"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("hobbies", "string"),
        schema("friends", "string"),
        schema("socialNetwork", "array"));
    verifyDataRows(
        result,
        rows("Brad Green", List.of("reading"), List.of("Shirley Soto"), Collections.emptyList()));
  }

  /** Test 7: Find friends network with depth tracking. */
  @Test
  public void testTravelersFriendsWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Brad Green'"
                    + " | graphLookup %s"
                    + " startField=friends"
                    + " fromField=friends"
                    + " toField=name"
                    + " depthField=connectionLevel"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("hobbies", "string"),
        schema("friends", "string"),
        schema("socialNetwork", "array"));
    verifyDataRows(
        result,
        rows("Brad Green", List.of("reading"), List.of("Shirley Soto"), Collections.emptyList()));
  }

  // ==================== Airport Connections Tests ====================

  /**
   * Test 8: Find all reachable airports from each airport. Note: Currently returns empty
   * reachableAirports arrays because the connects field is an array type, which the current
   * implementation doesn't fully traverse.
   */
  @Test
  public void testAirportConnections() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=connects"
                    + " fromField=connects"
                    + " toField=airport"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows("JFK", List.of("BOS", "ORD"), Collections.emptyList()),
        rows("BOS", List.of("JFK", "PWM"), Collections.emptyList()),
        rows("ORD", List.of("JFK"), Collections.emptyList()),
        rows("PWM", List.of("BOS", "LHR"), Collections.emptyList()),
        rows("LHR", List.of("PWM"), Collections.emptyList()));
  }

  /** Test 9: Find airports reachable from JFK within maxDepth=1. */
  @Test
  public void testAirportConnectionsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where airport = 'JFK'"
                    + " | graphLookup %s"
                    + " startField=connects"
                    + " fromField=connects"
                    + " toField=airport"
                    + " maxDepth=1"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(result, rows("JFK", List.of("BOS", "ORD"), Collections.emptyList()));
  }

  /** Test 10: Find airports with hop count tracked. */
  @Test
  public void testAirportConnectionsWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where airport = 'JFK'"
                    + " | graphLookup %s"
                    + " startField=connects"
                    + " fromField=connects"
                    + " toField=airport"
                    + " depthField=numConnections"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(result, rows("JFK", List.of("BOS", "ORD"), Collections.emptyList()));
  }

  // ==================== Bidirectional Traversal Tests ====================

  /** Test 11: Bidirectional traversal for Ron (finds both managers and reports). */
  @Test
  public void testBidirectionalEmployeeHierarchy() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Ron'"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " direction=bi"
                    + " as connections",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("connections", "array"));
    verifyDataRows(
        result,
        rows(
            "Ron",
            "Andrew",
            3,
            List.of("{Ron, Andrew, 3}", "{Andrew, null, 4}", "{Dan, Andrew, 6}")));
  }

  /**
   * Test 12: Bidirectional airport connections for ORD. Note: Currently returns empty
   * allConnections array because the connects field is an array type.
   */
  @Test
  public void testBidirectionalAirportConnections() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where airport = 'ORD'"
                    + " | graphLookup %s"
                    + " startField=connects"
                    + " fromField=connects"
                    + " toField=airport"
                    + " direction=bi"
                    + " as allConnections",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("allConnections", "array"));
    verifyDataRows(result, rows("ORD", List.of("JFK"), Collections.emptyList()));
  }

  // ==================== Edge Cases ====================

  /** Test 13: Graph lookup on empty result set (non-existent employee). */
  @Test
  public void testEmptySourceResult() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'NonExistent'"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(result);
  }

  /** Test 14: CEO (Andrew) with no manager - hierarchy should be empty. */
  @Test
  public void testEmployeeWithNoManager() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Andrew'"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    verifyDataRows(result, rows("Andrew", null, 4, Collections.emptyList()));
  }

  /** Test 15: Combined with stats command. */
  @Test
  public void testGraphLookupWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy"
                    + " | stats count() by name",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("count()", "bigint"), schema("name", "string"));
    verifyDataRows(
        result,
        rows(1L, "Ron"),
        rows(1L, "Dan"),
        rows(1L, "Dev"),
        rows(1L, "Andrew"),
        rows(1L, "Asya"),
        rows(1L, "Eliot"));
  }

  /** Test 16: Graph lookup with fields projection (name and reportingHierarchy only). */
  @Test
  public void testGraphLookupWithFieldsProjection() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " as reportingHierarchy"
                    + " | fields name, reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("name", "string"), schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows("Dev", List.of("{Eliot, Ron, 2}")),
        rows("Eliot", List.of("{Ron, Andrew, 3}")),
        rows("Ron", List.of("{Andrew, null, 4}")),
        rows("Andrew", Collections.emptyList()),
        rows("Asya", List.of("{Ron, Andrew, 3}")),
        rows("Dan", List.of("{Andrew, null, 4}")));
  }
}
