/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_AIRPORTS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_EMPLOYEES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_TRAVELERS;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
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
 *   <li>graph_employees: Employee hierarchy with reportsTo field (Dev -> Eliot -> Ron -> Andrew)
 *   <li>graph_travelers: Social network with friends connections
 *   <li>graph_airports: Airport connections graph (JFK, BOS, ORD, PWM, LHR)
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

  /**
   * Test 1: Basic employee hierarchy traversal. Find all managers in the reporting chain for each
   * employee. Similar to MongoDB example: "Within a Collection".
   *
   * <p>Employee hierarchy: Dev -> Eliot -> Ron -> Andrew (CEO) Asya -> Ron -> Andrew Dan -> Andrew
   */
  @Test
  public void testEmployeeHierarchyBasicTraversal() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // Should return 6 employees, each with their reporting hierarchy
    System.out.println(result);
    verifyNumOfRows(result, 6);
  }

  /**
   * Test 2: Employee hierarchy traversal with depth field. Track the depth of each manager in the
   * hierarchy.
   */
  @Test
  public void testEmployeeHierarchyWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " depthField=level"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    System.out.println(result);
    verifyNumOfRows(result, 6);
  }

  /**
   * Test 3: Employee hierarchy traversal with maxDepth limit. Only find managers up to 1 level
   * above.
   */
  @Test
  public void testEmployeeHierarchyWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " maxDepth=1"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // Each employee should have at most 2 managers in their hierarchy (depth 0 and 1)
    System.out.println(result);
    verifyNumOfRows(result, 6);
  }

  /**
   * Test 4: Query specific employee and find their complete reporting chain. Filter to Dev and find
   * all his managers.
   */
  @Test
  public void testEmployeeHierarchyForSpecificEmployee() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Dev'"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // Dev's hierarchy: Eliot, Ron, Andrew
    System.out.println(result);
    verifyNumOfRows(result, 1);
  }

  // ==================== Social Network (Travelers) Tests ====================

  /**
   * Test 5: Find all friends (direct and indirect) for travelers. Social network traversal - find
   * friends of friends.
   */
  @Test
  public void testTravelersFriendsNetwork() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=friends"
                    + " connectFromField=friends"
                    + " connectToField=name"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    // All 4 travelers with their social networks
    System.out.println(result);
    verifyNumOfRows(result, 4);
  }

  /**
   * Test 6: Find friends network with limited depth. Only get direct friends (depth 0) and friends
   * of friends (depth 1).
   */
  @Test
  public void testTravelersFriendsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Brad Green'"
                    + " | graphLookup %s"
                    + " startWith=friends"
                    + " connectFromField=friends"
                    + " connectToField=name"
                    + " maxDepth=1"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    // Brad Green -> Shirley Soto -> {Tanya Jordan, Terry Hawkins}
    System.out.println(result);
    verifyNumOfRows(result, 1);
  }

  /**
   * Test 7: Find friends network with depth tracking. Track the degree of connection for each
   * friend.
   */
  @Test
  public void testTravelersFriendsWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=friends"
                    + " connectFromField=friends"
                    + " connectToField=name"
                    + " depthField=connectionLevel"
                    + " as socialNetwork",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_TRAVELERS));

    System.out.println(result);
    verifyNumOfRows(result, 4);
  }

  // ==================== Airport Connections Tests ====================

  /**
   * Test 8: Find all reachable airports from each airport. Similar to MongoDB example: "Within
   * Collection with maxDepth".
   */
  @Test
  public void testAirportConnections() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=connects"
                    + " connectFromField=connects"
                    + " connectToField=airport"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    System.out.println(result);
    verifyNumOfRows(result, 5);
  }

  /**
   * Test 9: Find airports reachable within 1 connection. Limited traversal depth.
   */
  @Test
  public void testAirportConnectionsWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where airport = 'JFK'"
                    + " | graphLookup %s"
                    + " startWith=connects"
                    + " connectFromField=connects"
                    + " connectToField=airport"
                    + " maxDepth=1"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    // JFK -> {BOS, ORD} (depth 0) -> {JFK, PWM} (depth 1, excluding JFK as already visited)
    System.out.println(result);
    verifyNumOfRows(result, 1);
  }

  /**
   * Test 10: Find airports with number of connections (hops) tracked.
   */
  @Test
  public void testAirportConnectionsWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=connects"
                    + " connectFromField=connects"
                    + " connectToField=airport"
                    + " depthField=numConnections"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    System.out.println(result);
    verifyNumOfRows(result, 5);
  }

  // ==================== Bidirectional Traversal Tests ====================

  /**
   * Test 11: Bidirectional traversal on employee hierarchy. Find both reports-to and direct-reports
   * relationships.
   */
  @Test
  public void testBidirectionalEmployeeHierarchy() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Ron'"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " direction=bi"
                    + " as connections",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // Ron should find both his managers (Andrew) and his reports (Eliot, Asya, and indirectly Dev)
    System.out.println(result);
    verifyNumOfRows(result, 1);
  }

  /**
   * Test 12: Bidirectional airport connections. Find all airports connected in either direction.
   */
  @Test
  public void testBidirectionalAirportConnections() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=connects"
                    + " connectFromField=connects"
                    + " connectToField=airport"
                    + " direction=bi"
                    + " as allConnections",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifyNumOfRows(result, 5);
  }

  // ==================== Edge Cases ====================

  /**
   * Test 13: Graph lookup on empty result set. Filter to non-existent employee.
   */
  @Test
  public void testEmptySourceResult() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'NonExistent'"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    System.out.println(result);
    verifyNumOfRows(result, 0);
  }

  /**
   * Test 14: Employee at top of hierarchy (CEO with no manager). Andrew has no reportsTo, so his
   * hierarchy should be empty.
   */
  @Test
  public void testEmployeeWithNoManager() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Andrew'"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // Andrew is CEO, no one above him
    System.out.println(result);
    verifyNumOfRows(result, 1);
  }

  /**
   * Test 15: Combined with other PPL commands (stats, sort). Count employees by hierarchy depth.
   */
  @Test
  public void testGraphLookupWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy"
                    + " | stats count() by name",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    // 6 distinct employees
    System.out.println(result);
    verifyNumOfRows(result, 6);
  }

  /**
   * Test 16: Graph lookup with fields projection. Only select specific fields in the result.
   */
  @Test
  public void testGraphLookupWithFieldsProjection() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " startWith=reportsTo"
                    + " connectFromField=reportsTo"
                    + " connectToField=name"
                    + " as reportingHierarchy"
                    + " | fields name, reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    System.out.println(result);
    verifyNumOfRows(result, 6);
  }
}
