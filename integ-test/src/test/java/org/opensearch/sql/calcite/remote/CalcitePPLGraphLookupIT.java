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
 *   <li>graph_travelers: Travelers with nearest airport (Dev->JFK, Eliot->JFK, Jeff->BOS)
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

  // ==================== Airport Connections Tests ====================

  /** Test 5: Find all reachable airports from each airport. */
  @Test
  public void testAirportConnections() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=airport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " supportArray=true"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows("JFK", List.of("BOS", "ORD"), List.of("{JFK, [BOS, ORD]}")),
        rows("BOS", List.of("JFK", "PWM"), List.of("{BOS, [JFK, PWM]}")),
        rows("ORD", List.of("JFK"), List.of("{ORD, [JFK]}")),
        rows("PWM", List.of("BOS", "LHR"), List.of("{PWM, [BOS, LHR]}")),
        rows("LHR", List.of("PWM"), List.of("{LHR, [PWM]}")));
  }

  /** Test 6: Find airports reachable from JFK within maxDepth=1. */
  @Test
  public void testAirportConnectionsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where airport = 'JFK'"
                    + " | graphLookup %s"
                    + " startField=airport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " maxDepth=1"
                    + " supportArray=true"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows("JFK", List.of("BOS", "ORD"), List.of("{JFK, [BOS, ORD]}", "{BOS, [JFK, PWM]}")));
  }

  /** Test 7: Find airports with default depth(=0) and start value of list */
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
    verifyDataRows(result, rows("JFK", List.of("BOS", "ORD"), List.of("{BOS, [JFK, PWM], 0}")));
  }

  /**
   * Test 8: Find reachable airports for all travelers. Uses travelers as source and airports as
   * lookup table, with nearestAirport as the starting point for graph traversal.
   */
  @Test
  public void testTravelersReachableAirports() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=nearestAirport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("nearestAirport", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows("Dev", "JFK", List.of("{JFK, [BOS, ORD]}")),
        rows("Eliot", "JFK", List.of("{JFK, [BOS, ORD]}")),
        rows("Jeff", "BOS", List.of("{BOS, [JFK, PWM]}")));
  }

  /**
   * Test 9: Find reachable airports for a specific traveler (Dev at JFK) with depth tracking.
   * Traverses from JFK through connected airports.
   */
  @Test
  public void testTravelerReachableAirportsWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Dev'"
                    + " | graphLookup %s"
                    + " startField=nearestAirport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " depthField=hops"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("nearestAirport", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(result, rows("Dev", "JFK", List.of("{JFK, [BOS, ORD], 0}")));
  }

  /**
   * Test 10: Find reachable airports for Jeff (at BOS) with maxDepth=1. Finds BOS record as the
   * starting point and traverses one level to connected airports.
   */
  @Test
  public void testTravelerReachableAirportsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Jeff'"
                    + " | graphLookup %s"
                    + " startField=nearestAirport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " maxDepth=1"
                    + " supportArray=true"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("nearestAirport", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows(
            "Jeff", "BOS", List.of("{BOS, [JFK, PWM]}", "{JFK, [BOS, ORD]}", "{PWM, [BOS, LHR]}")));
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
    verifyDataRows(
        result, rows("ORD", List.of("JFK"), List.of("{JFK, [BOS, ORD]}", "{BOS, [JFK, PWM]}")));
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

  // ==================== Batch Mode Tests ====================

  /**
   * Test 17: Batch mode - collects all start values and performs unified BFS. Output is a single
   * row with [Array<source>, Array<lookup>].
   *
   * <p>Source: Dev (reportsTo=Eliot), Asya (reportsTo=Ron) Start values: {Eliot, Ron} BFS finds:
   * Eliot->Ron, Ron->Andrew, Andrew->null
   */
  @Test
  public void testBatchModeEmployeeHierarchy() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name in ('Dev', 'Asya')"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " depthField=depth"
                    + " maxDepth=3"
                    + " batchMode=true"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportsTo", "array"), schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows(
            List.of("{Dev, Eliot, 1}", "{Asya, Ron, 5}"),
            List.of("{Ron, Andrew, 3, 0}", "{Andrew, null, 4, 1}")));
  }

  /**
   * Test 18: Batch mode for travelers - find all airports reachable from any traveler. All
   * travelers' nearest airports: JFK (Dev, Eliot), BOS (Jeff) Unified BFS from {JFK, BOS} with
   * maxDepth=1 finds connected airports.
   */
  @Test
  public void testBatchModeTravelersAirports() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startField=nearestAirport"
                    + " fromField=connects"
                    + " toField=airport"
                    + " batchMode=true"
                    + " depthField=depth"
                    + " maxDepth=3"
                    + " supportArray=true"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(result, schema("nearestAirport", "array"), schema("reachableAirports", "array"));
    // Batch mode returns single row with:
    // - sourceRows: [{Dev, JFK}, {Eliot, JFK}, {Jeff, BOS}]
    // - lookupResults: airports reachable from JFK and BOS within maxDepth=1
    verifyDataRows(
        result,
        rows(
            List.of("{Dev, JFK}", "{Eliot, JFK}", "{Jeff, BOS}"),
            List.of("{JFK, [BOS, ORD], 0}", "{BOS, [JFK, PWM], 0}", "{PWM, [BOS, LHR], 1}")));
  }

  /**
   * Test 19: Batch mode with bidirectional traversal. Dev (reportsTo=Eliot), Dan (reportsTo=Andrew)
   * Bidirectional BFS from {Eliot, Andrew} finds connections in both directions.
   */
  @Test
  public void testBatchModeBidirectional() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name in ('Dev', 'Dan')"
                    + " | graphLookup %s"
                    + " startField=reportsTo"
                    + " fromField=reportsTo"
                    + " toField=name"
                    + " depthField=depth"
                    + " maxDepth=3"
                    + " direction=bi"
                    + " batchMode=true"
                    + " as connections",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportsTo", "array"), schema("connections", "array"));
    // Batch mode returns single row with bidirectional traversal results
    // Start from {Eliot, Andrew}, find connections in both directions
    verifyDataRows(
        result,
        rows(
            List.of("{Dev, Eliot, 1}", "{Dan, Andrew, 6}"),
            List.of(
                "{Dev, Eliot, 1, 0}",
                "{Eliot, Ron, 2, 0}",
                "{Andrew, null, 4, 0}",
                "{Dan, Andrew, 6, 0}",
                "{Asya, Ron, 5, 1}")));
  }
}
