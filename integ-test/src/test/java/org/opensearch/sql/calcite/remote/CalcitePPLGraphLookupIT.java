/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_AIRPORTS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_EMPLOYEES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GRAPH_TRAVELERS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONArray;
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
    // Skip test if pushdown is disabled
    // TODO: support no-pushdown config for graph lookup
    enabledOnlyWhenPushdownIsEnabled();

    loadIndex(Index.GRAPH_EMPLOYEES);
    loadIndex(Index.GRAPH_TRAVELERS);
    loadIndex(Index.GRAPH_AIRPORTS);
  }

  /** Null-safe map helper since {@code Map.of()} rejects null values. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> mapOf(Object... keysAndValues) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  /**
   * Row matcher for graphLookup results whose collected arrays come back in a shard-dependent
   * order.
   *
   * <p>graphLookup gathers traversal results into an array, and the order of elements within that
   * array is not defined across shards. A multi-shard run therefore returns the same set of nodes
   * (same values, same {@code depth}/{@code numConnections}, same cardinality) as a single-shard
   * run, only in a different order. Unlike {@link
   * org.opensearch.sql.util.MatcherUtils#rows(Object...)}, which compares row cells with
   * order-sensitive {@link JSONArray#similar}, this matcher relaxes ordering <em>only within nested
   * arrays and objects</em>. The top-level row cells are still compared positionally: cell {@code
   * i} of the actual row must match cell {@code i} of the expected row. This preserves column
   * identity, so a swap of two same-typed top-level columns is still rejected, while collected
   * array order may vary across shards. It is scoped to this test class so the relaxed comparison
   * never leaks into other suites.
   */
  private static TypeSafeMatcher<JSONArray> rowsUnordered(Object... expectedObjects) {
    return new TypeSafeMatcher<>() {
      @Override
      protected boolean matchesSafely(JSONArray array) {
        JSONArray expected = new JSONArray(expectedObjects);
        if (array.length() != expected.length()) {
          return false;
        }
        // Compare top-level cells positionally so column identity is preserved; only descend into
        // nested arrays/objects with order-insensitive comparison.
        for (int i = 0; i < expected.length(); i++) {
          if (!jsonEqualsIgnoringOrder(array.get(i), expected.get(i))) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(new JSONArray(expectedObjects).toString());
      }
    };
  }

  /** Recursively compares two JSON values, treating every array as an unordered multiset. */
  private static boolean jsonEqualsIgnoringOrder(Object a, Object b) {
    if (a instanceof JSONArray && b instanceof JSONArray) {
      return jsonArrayEqualsIgnoringNestedOrder((JSONArray) a, (JSONArray) b);
    }
    if (a instanceof JSONObject && b instanceof JSONObject) {
      JSONObject ao = (JSONObject) a;
      JSONObject bo = (JSONObject) b;
      if (ao.keySet().size() != bo.keySet().size()) {
        return false;
      }
      for (String key : ao.keySet()) {
        if (!bo.has(key) || !jsonEqualsIgnoringOrder(ao.get(key), bo.get(key))) {
          return false;
        }
      }
      return true;
    }
    return scalarEquals(a, b);
  }

  private static boolean jsonArrayEqualsIgnoringNestedOrder(JSONArray actual, JSONArray expected) {
    if (actual.length() != expected.length()) {
      return false;
    }
    List<Object> remaining = new ArrayList<>();
    for (int i = 0; i < expected.length(); i++) {
      remaining.add(expected.get(i));
    }
    for (int i = 0; i < actual.length(); i++) {
      Object actualElement = actual.get(i);
      boolean matched = false;
      for (Iterator<Object> it = remaining.iterator(); it.hasNext(); ) {
        if (jsonEqualsIgnoringOrder(actualElement, it.next())) {
          it.remove();
          matched = true;
          break;
        }
      }
      if (!matched) {
        return false;
      }
    }
    return true;
  }

  private static boolean scalarEquals(Object a, Object b) {
    boolean aNull = a == null || a == JSONObject.NULL;
    boolean bNull = b == null || b == JSONObject.NULL;
    if (aNull || bNull) {
      return aNull && bNull;
    }
    if (a instanceof Number && b instanceof Number) {
      return ((Number) a).doubleValue() == ((Number) b).doubleValue();
    }
    return a.toString().equals(b.toString());
  }

  /**
   * Regression test for {@link #rowsUnordered(Object...)}. Proves the matcher relaxes ordering only
   * inside nested arrays/objects while keeping top-level column positions and cardinality
   * significant. This runs without a cluster so it always executes as part of the suite.
   */
  @Test
  public void testRowsUnorderedMatcherPreservesColumnPositions() {
    // 1. A shard-dependent reorder inside a nested collected array still matches.
    Matcher<JSONArray> matcher = rowsUnordered("Jeff", List.of("BOS", "JFK", "LAX"));
    JSONArray nestedReordered = new JSONArray(List.of("Jeff", List.of("LAX", "BOS", "JFK")));
    assertTrue("nested array reorder should match", matcher.matches(nestedReordered));

    // 2. A swap of two same-typed top-level columns must NOT match (column identity preserved).
    Matcher<JSONArray> swapMatcher = rowsUnordered("BOS", "JFK");
    JSONArray swapped = new JSONArray(List.of("JFK", "BOS"));
    assertFalse("same-type top-level column swap must not match", swapMatcher.matches(swapped));

    // 3. A missing value inside the nested array (different cardinality) must NOT match.
    JSONArray missingNested = new JSONArray(List.of("Jeff", List.of("BOS", "JFK")));
    assertFalse("missing nested element must not match", matcher.matches(missingNested));
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
        rows("Dev", "Eliot", 1, List.of(Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2))),
        rows("Eliot", "Ron", 2, List.of(Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))),
        rows("Ron", "Andrew", 3, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows("Asya", "Ron", 5, List.of(Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))),
        rows("Dan", "Andrew", 6, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
  }

  /** Test 2: Employee hierarchy traversal with depth field. */
  @Test
  public void testEmployeeHierarchyWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
        rows(
            "Dev",
            "Eliot",
            1,
            List.of(mapOf("name", "Eliot", "reportsTo", "Ron", "id", 2, "level", 0))),
        rows(
            "Eliot",
            "Ron",
            2,
            List.of(mapOf("name", "Ron", "reportsTo", "Andrew", "id", 3, "level", 0))),
        rows(
            "Ron",
            "Andrew",
            3,
            List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4, "level", 0))),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows(
            "Asya",
            "Ron",
            5,
            List.of(mapOf("name", "Ron", "reportsTo", "Andrew", "id", 3, "level", 0))),
        rows(
            "Dan",
            "Andrew",
            6,
            List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4, "level", 0))));
  }

  /** Test 3: Employee hierarchy with maxDepth=1 (allows 2 levels of traversal). */
  @Test
  public void testEmployeeHierarchyWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
        rows(
            "Dev",
            "Eliot",
            1,
            List.of(
                Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2),
                Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))),
        rows(
            "Eliot",
            "Ron",
            2,
            List.of(
                Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3),
                mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Ron", "Andrew", 3, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows(
            "Asya",
            "Ron",
            5,
            List.of(
                Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3),
                mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Dan", "Andrew", 6, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
        rows("Dev", "Eliot", 1, List.of(Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2))));
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
                    + " start=airport"
                    + " edge=connects-->airport"
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
        rows(
            "JFK",
            List.of("BOS", "ORD"),
            List.of(Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")))),
        rows(
            "BOS",
            List.of("JFK", "PWM"),
            List.of(Map.of("airport", "BOS", "connects", List.of("JFK", "PWM")))),
        rows("ORD", List.of("JFK"), List.of(Map.of("airport", "ORD", "connects", List.of("JFK")))),
        rows(
            "PWM",
            List.of("BOS", "LHR"),
            List.of(Map.of("airport", "PWM", "connects", List.of("BOS", "LHR")))),
        rows("LHR", List.of("PWM"), List.of(Map.of("airport", "LHR", "connects", List.of("PWM")))));
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
                    + " start=airport"
                    + " edge=connects-->airport"
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
        rows(
            "JFK",
            List.of("BOS", "ORD"),
            List.of(
                Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")),
                Map.of("airport", "BOS", "connects", List.of("JFK", "PWM")))));
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
                    + " start=connects"
                    + " edge=connects-->airport"
                    + " depthField=numConnections"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));
    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows(
            "JFK",
            List.of("BOS", "ORD"),
            List.of(
                mapOf("airport", "BOS", "connects", List.of("JFK", "PWM"), "numConnections", 0))));
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
                    + " start=nearestAirport"
                    + " edge=connects-->airport"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("nearestAirport", "string"),
        schema("reachableAirports", "array"));
    verifyDataRows(
        result,
        rows("Dev", "JFK", List.of(Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")))),
        rows("Eliot", "JFK", List.of(Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")))),
        rows("Jeff", "BOS", List.of(Map.of("airport", "BOS", "connects", List.of("JFK", "PWM")))));
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
                    + " start=nearestAirport"
                    + " edge=connects-->airport"
                    + " depthField=hops"
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
            "Dev",
            "JFK",
            List.of(mapOf("airport", "JFK", "connects", List.of("BOS", "ORD"), "hops", 0))));
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
                    + " start=nearestAirport"
                    + " edge=connects-->airport"
                    + " maxDepth=1"
                    + " supportArray=true"
                    + " as reachableAirports",
                TEST_INDEX_GRAPH_TRAVELERS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("name", "string"),
        schema("nearestAirport", "string"),
        schema("reachableAirports", "array"));
    // graphLookup collects reachable airports into an array whose element order is shard-dependent;
    // compare values/connects/cardinality while ignoring nested array order.
    verifyDataRows(
        result,
        rowsUnordered(
            "Jeff",
            "BOS",
            List.of(
                Map.of("airport", "BOS", "connects", List.of("JFK", "PWM")),
                Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")),
                Map.of("airport", "PWM", "connects", List.of("BOS", "LHR")))));
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
                    + " start=reportsTo"
                    + " edge=reportsTo<->name"
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
            List.of(
                Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3),
                mapOf("name", "Andrew", "reportsTo", null, "id", 4),
                Map.of("name", "Dan", "reportsTo", "Andrew", "id", 6))));
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
                    + " start=connects"
                    + " edge=connects<->airport"
                    + " as allConnections",
                TEST_INDEX_GRAPH_AIRPORTS, TEST_INDEX_GRAPH_AIRPORTS));

    verifySchema(
        result,
        schema("airport", "string"),
        schema("connects", "string"),
        schema("allConnections", "array"));
    verifyDataRows(
        result,
        rows(
            "ORD",
            List.of("JFK"),
            List.of(
                Map.of("airport", "JFK", "connects", List.of("BOS", "ORD")),
                Map.of("airport", "BOS", "connects", List.of("JFK", "PWM")))));
  }

  // ==================== Filter Tests ====================

  /**
   * Test: Filter employee hierarchy by id. Only lookup documents with id > 3 (Andrew=4, Asya=5,
   * Dan=6) participate in traversal. Dev starts with reportsTo=Eliot, but Eliot (id=2) is excluded
   * by filter, so Dev gets empty results.
   */
  @Test
  public void testEmployeeHierarchyWithFilter() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | graphLookup %s"
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
                    + " filter=(id > 3)"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    // Only documents with id > 3 (Andrew=4, Asya=5, Dan=6) are in lookup table
    // Dev: reportsTo=Eliot -> Eliot(id=2) is filtered out -> empty
    // Eliot: reportsTo=Ron -> Ron(id=3) is filtered out -> empty
    // Ron: reportsTo=Andrew -> Andrew(id=4) passes filter -> [{Andrew, null, 4}]
    // Andrew: reportsTo=null -> empty
    // Asya: reportsTo=Ron -> Ron(id=3) is filtered out -> empty
    // Dan: reportsTo=Andrew -> Andrew(id=4) passes filter -> [{Andrew, null, 4}]
    verifyDataRows(
        result,
        rows("Dev", "Eliot", 1, Collections.emptyList()),
        rows("Eliot", "Ron", 2, Collections.emptyList()),
        rows("Ron", "Andrew", 3, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Andrew", null, 4, Collections.emptyList()),
        rows("Asya", "Ron", 5, Collections.emptyList()),
        rows("Dan", "Andrew", 6, List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
  }

  /**
   * Test: Filter employee hierarchy with keyword match. Only employees whose name is NOT 'Andrew'
   * participate in traversal.
   */
  @Test
  public void testEmployeeHierarchyWithKeywordFilter() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Ron'"
                    + " | graphLookup %s"
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
                    + " filter=(name != 'Andrew')"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    // Ron: reportsTo=Andrew -> Andrew is filtered out by name != 'Andrew' -> empty
    verifyDataRows(result, rows("Ron", "Andrew", 3, Collections.emptyList()));
  }

  /**
   * Test: Filter with maxDepth combined. Dev traverses reporting chain but only considers lookup
   * documents with id <= 3.
   */
  @Test
  public void testEmployeeHierarchyWithFilterAndMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | where name = 'Dev'"
                    + " | graphLookup %s"
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
                    + " maxDepth=3"
                    + " filter=(id <= 3)"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(
        result,
        schema("name", "string"),
        schema("reportsTo", "string"),
        schema("id", "int"),
        schema("reportingHierarchy", "array"));
    // Dev: reportsTo=Eliot -> Eliot(id=2) passes -> then Eliot.reportsTo=Ron -> Ron(id=3) passes
    // -> then Ron.reportsTo=Andrew -> Andrew(id=4) is filtered out -> stops
    verifyDataRows(
        result,
        rows(
            "Dev",
            "Eliot",
            1,
            List.of(
                Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2),
                Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))));
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
                    + " as reportingHierarchy"
                    + " | fields name, reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("name", "string"), schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows("Dev", List.of(Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2))),
        rows("Eliot", List.of(Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))),
        rows("Ron", List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))),
        rows("Andrew", Collections.emptyList()),
        rows("Asya", List.of(Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3))),
        rows("Dan", List.of(mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
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
                    + " start=reportsTo"
                    + " edge=reportsTo-->name"
                    + " depthField=depth"
                    + " maxDepth=3"
                    + " batchMode=true"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportsTo", "array"), schema("reportingHierarchy", "array"));
    // Batch mode returns a single row; the source-rows array and the hierarchy array both come
    // back in a shard-dependent order, so compare them as unordered multisets.
    verifyDataRows(
        result,
        rowsUnordered(
            List.of(
                Map.of("name", "Dev", "reportsTo", "Eliot", "id", 1),
                Map.of("name", "Asya", "reportsTo", "Ron", "id", 5)),
            List.of(
                mapOf("name", "Ron", "reportsTo", "Andrew", "id", 3, "depth", 0),
                mapOf("name", "Andrew", "reportsTo", null, "id", 4, "depth", 1))));
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
                    + " start=nearestAirport"
                    + " edge=connects-->airport"
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
        rowsUnordered(
            List.of(
                Map.of("name", "Dev", "nearestAirport", "JFK"),
                Map.of("name", "Eliot", "nearestAirport", "JFK"),
                Map.of("name", "Jeff", "nearestAirport", "BOS")),
            List.of(
                mapOf("airport", "JFK", "connects", List.of("BOS", "ORD"), "depth", 0),
                mapOf("airport", "BOS", "connects", List.of("JFK", "PWM"), "depth", 0),
                mapOf("airport", "PWM", "connects", List.of("BOS", "LHR"), "depth", 1))));
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
                    + " start=reportsTo"
                    + " edge=reportsTo<->name"
                    + " depthField=depth"
                    + " maxDepth=3"
                    + " batchMode=true"
                    + " as connections",
                TEST_INDEX_GRAPH_EMPLOYEES, TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportsTo", "array"), schema("connections", "array"));
    // Batch mode returns single row with bidirectional traversal results
    // Start from {Eliot, Andrew}, find connections in both directions
    verifyDataRows(
        result,
        rows(
            List.of(
                Map.of("name", "Dev", "reportsTo", "Eliot", "id", 1),
                Map.of("name", "Dan", "reportsTo", "Andrew", "id", 6)),
            List.of(
                mapOf("name", "Dev", "reportsTo", "Eliot", "id", 1, "depth", 0),
                mapOf("name", "Eliot", "reportsTo", "Ron", "id", 2, "depth", 0),
                mapOf("name", "Andrew", "reportsTo", null, "id", 4, "depth", 0),
                mapOf("name", "Dan", "reportsTo", "Andrew", "id", 6, "depth", 0),
                mapOf("name", "Asya", "reportsTo", "Ron", "id", 5, "depth", 1))));
  }

  // ==================== Top-Level Literal Start Tests ====================

  /**
   * Test 20: Top-level graphLookup with single literal start value. BFS from "Eliot" finds the
   * reporting chain: Eliot->Ron->Andrew.
   */
  @Test
  public void testTopLevelGraphLookupSingleLiteral() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "graphLookup %s"
                    + " start='Eliot'"
                    + " edge=reportsTo-->name"
                    + " maxDepth=5"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES));

    // Output is single row with just the reportingHierarchy array
    verifySchema(result, schema("reportingHierarchy", "array"));
    // BFS from "Eliot": toField=name matches Eliot -> Eliot row has reportsTo=Ron
    // -> then name matches Ron -> Ron row has reportsTo=Andrew
    // -> then name matches Andrew -> Andrew row has reportsTo=null (no further traversal)
    verifyDataRows(
        result,
        rows(
            (Object)
                List.of(
                    Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2),
                    Map.of("name", "Ron", "reportsTo", "Andrew", "id", 3),
                    mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
  }

  /**
   * Test 21: Top-level graphLookup with literal list start values. Combined BFS from "Eliot" and
   * "Andrew".
   */
  @Test
  public void testTopLevelGraphLookupLiteralList() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "graphLookup %s"
                    + " start='Eliot', 'Andrew'"
                    + " edge=reportsTo-->name"
                    + " maxDepth=5"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportingHierarchy", "array"));
    // Combined BFS from {Eliot, Andrew}:
    // Depth 0: name IN (Eliot, Andrew) → finds Eliot (reportsTo=Ron) and Andrew (reportsTo=null)
    // Depth 1: name IN (Ron) AND reportsTo NOT IN (Eliot, Andrew, Ron) → Ron excluded
    //   because Ron.reportsTo=Andrew is in visited set
    verifyDataRows(
        result,
        rowsUnordered(
            (Object)
                List.of(
                    Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2),
                    mapOf("name", "Andrew", "reportsTo", null, "id", 4))));
  }

  /** Test 22: Top-level graphLookup with maxDepth. */
  @Test
  public void testTopLevelGraphLookupWithMaxDepth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "graphLookup %s"
                    + " start='Eliot'"
                    + " edge=reportsTo-->name"
                    + " maxDepth=0"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportingHierarchy", "array"));
    // maxDepth=0: Only immediate match for "Eliot" (Eliot row), no further traversal
    verifyDataRows(
        result, rows((Object) List.of(Map.of("name", "Eliot", "reportsTo", "Ron", "id", 2))));
  }

  /** Test 23: Top-level graphLookup with depthField and maxDepth. */
  @Test
  public void testTopLevelGraphLookupWithDepthField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "graphLookup %s"
                    + " start='Eliot'"
                    + " edge=reportsTo-->name"
                    + " depthField=level"
                    + " maxDepth=5"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportingHierarchy", "array"));
    verifyDataRows(
        result,
        rows(
            (Object)
                List.of(
                    mapOf("name", "Eliot", "reportsTo", "Ron", "id", 2, "level", 0),
                    mapOf("name", "Ron", "reportsTo", "Andrew", "id", 3, "level", 1),
                    mapOf("name", "Andrew", "reportsTo", null, "id", 4, "level", 2))));
  }

  /** Test 24: Top-level graphLookup with non-existent start value yields empty results. */
  @Test
  public void testTopLevelGraphLookupNonExistentStart() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "graphLookup %s"
                    + " start='NonExistent'"
                    + " edge=reportsTo-->name"
                    + " as reportingHierarchy",
                TEST_INDEX_GRAPH_EMPLOYEES));

    verifySchema(result, schema("reportingHierarchy", "array"));
    verifyDataRows(result, rows((Object) Collections.emptyList()));
  }
}
