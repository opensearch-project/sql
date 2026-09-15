/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.AbstractAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownOperation;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;

/**
 * Unit tests for the BFS traversal semantics of {@link CalciteEnumerableGraphLookup}.
 *
 * <p>The lookup index is replaced by an in-memory fake that evaluates the terms query the
 * enumerator pushes down against a fixed document list, and returns matches in a configurable
 * order. This exercises the three traversal invariants without a cluster:
 *
 * <ul>
 *   <li><b>All-reached emission</b>: every matched document is emitted exactly once at its minimum
 *       depth, even when all of its onward edges point to already-visited nodes.
 *   <li><b>Order independence</b>: the emitted result set is identical regardless of the order in
 *       which the (fake) shards return rows within a BFS level.
 *   <li><b>Structural document identity</b>: two distinct documents sharing a connectTo value are
 *       both emitted; the same document rediscovered at a deeper level is emitted only once.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class CalciteEnumerableGraphLookupTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  /** Mirrors the graph_airports integration fixture: fields [airport, connects]. */
  private static final List<String> AIRPORT_FIELDS = List.of("airport", "connects");

  /** Mirrors the graph_employees integration fixture: fields [name, reportsTo]. */
  private static final List<String> EMPLOYEE_FIELDS = List.of("name", "reportsTo");

  @Mock private RelOptCluster cluster;
  @Mock private RelTraitSet traitSet;
  @Mock private RelOptTable table;
  @Mock private OpenSearchIndex osIndex;

  @BeforeEach
  void setUp() {
    lenient().when(osIndex.getMaxResultWindow()).thenReturn(10000);
    lenient().when(osIndex.getFieldTypes()).thenReturn(new HashMap<>());
  }

  private static List<Object[]> airportDocs() {
    return List.of(
        new Object[] {"JFK", List.of("BOS", "ORD")},
        new Object[] {"BOS", List.of("JFK", "PWM")},
        new Object[] {"ORD", List.of("JFK")},
        new Object[] {"PWM", List.of("BOS", "LHR")},
        new Object[] {"LHR", List.of("PWM")});
  }

  private static List<Object[]> employeeDocs() {
    List<Object[]> docs = new ArrayList<>();
    docs.add(new Object[] {"Dev", "Eliot"});
    docs.add(new Object[] {"Eliot", "Ron"});
    docs.add(new Object[] {"Ron", "Andrew"});
    docs.add(new Object[] {"Andrew", null});
    docs.add(new Object[] {"Asya", "Ron"});
    docs.add(new Object[] {"Dan", "Andrew"});
    return docs;
  }

  // ---------------------------------------------------------------------------------------------
  // All-reached emission and minimum-depth assignment
  // ---------------------------------------------------------------------------------------------

  @Test
  void allMatchedDocumentsAreEmittedAtTheirMinimumDepth() {
    // start=JFK.connects=[BOS, ORD], edge connects-->airport, like
    // testAirportConnectionsWithDepthField.
    List<Object[]> results =
        runLiteralBfs(
            airportDocs(),
            AIRPORT_FIELDS,
            List.of("BOS", "ORD"),
            "connects",
            "airport",
            "depth",
            10,
            false,
            UnaryOperator.identity());

    // JFK is reached at depth 1 while both of its onward edges (BOS, ORD) are already visited:
    // the old emission gate (`!nextValues.isEmpty()`) silently dropped it. All-reached semantics
    // must surface it, and every node exactly once at its shallowest depth.
    assertEquals(
        Map.of("BOS", 0, "ORD", 0, "JFK", 1, "PWM", 1, "LHR", 2),
        depthByKey(results),
        "every reached document must be emitted exactly once at its minimum BFS depth");
  }

  @Test
  void maxDepthBoundsTheTraversal() {
    List<Object[]> results =
        runLiteralBfs(
            airportDocs(),
            AIRPORT_FIELDS,
            List.of("BOS", "ORD"),
            "connects",
            "airport",
            "depth",
            0,
            false,
            UnaryOperator.identity());

    assertEquals(Map.of("BOS", 0, "ORD", 0), depthByKey(results));
  }

  // ---------------------------------------------------------------------------------------------
  // Order independence
  // ---------------------------------------------------------------------------------------------

  @Test
  void resultSetIsIndependentOfRowReturnOrderWithinALevel() {
    // The 1-shard vs 5-shard divergence reduced to its mechanism: the same query returning the
    // same matches in a different order. Both orders must produce the same set.
    UnaryOperator<List<Object[]>> reversed =
        matches -> {
          Collections.reverse(matches);
          return matches;
        };

    List<Object[]> insertionOrder =
        runLiteralBfs(
            airportDocs(),
            AIRPORT_FIELDS,
            List.of("BOS", "ORD"),
            "connects",
            "airport",
            "depth",
            10,
            false,
            UnaryOperator.identity());
    List<Object[]> reverseOrder =
        runLiteralBfs(
            airportDocs(),
            AIRPORT_FIELDS,
            List.of("BOS", "ORD"),
            "connects",
            "airport",
            "depth",
            10,
            false,
            reversed);

    assertEquals(depthByKey(insertionOrder), depthByKey(reverseOrder));
    // Both same-level documents survive; the old code emitted only the one that arrived first.
    assertEquals(2, countAtDepth(reverseOrder, 0), "both level-0 documents must be emitted");
  }

  @Test
  void bidirectionalTraversalEmitsEveryReachedDocumentOnce() {
    // start=ORD.connects=[JFK], edge connects<->airport, like testBidirectionalAirportConnections.
    // Depth 0 reaches JFK by airport plus BOS and ORD by connects -- three documents; the old code
    // emitted only two, decided by arrival order. Deeper levels rediscover all three, which must
    // dedup against the depth-0 emissions instead of duplicating them.
    for (UnaryOperator<List<Object[]>> order :
        List.<UnaryOperator<List<Object[]>>>of(
            UnaryOperator.identity(),
            matches -> {
              Collections.reverse(matches);
              return matches;
            })) {
      List<Object[]> results =
          runLiteralBfs(
              airportDocs(),
              AIRPORT_FIELDS,
              List.of("JFK"),
              "connects",
              "airport",
              "depth",
              10,
              true,
              order);

      assertEquals(
          Map.of("JFK", 0, "BOS", 0, "ORD", 0, "PWM", 1, "LHR", 1),
          depthByKey(results),
          "all three depth-0 documents must be emitted, later rediscoveries deduplicated");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Structural document identity
  // ---------------------------------------------------------------------------------------------

  @Test
  void distinctDocumentsSharingAConnectToValueAreBothEmitted() {
    // Two different documents with the same toField value. Keying identity on the toField value
    // (the old behavior) collapses them; the structural key over all columns must not.
    List<Object[]> docs =
        List.of(new Object[] {"HUB", List.of("X")}, new Object[] {"HUB", List.of("Y")});

    List<Object[]> results =
        runLiteralBfs(
            docs,
            AIRPORT_FIELDS,
            List.of("HUB"),
            "connects",
            "airport",
            null,
            10,
            false,
            UnaryOperator.identity());

    assertEquals(2, results.size(), "distinct documents sharing a toField value must not collapse");
    Set<Object> connects = new HashSet<>();
    for (Object[] row : results) {
      connects.add(row[1]);
    }
    assertEquals(Set.of(List.of("X"), List.of("Y")), connects);
  }

  // ---------------------------------------------------------------------------------------------
  // Batch and normal source modes
  // ---------------------------------------------------------------------------------------------

  @Test
  void batchModeRunsOneUnifiedBfsOverAllSourceStartValues() {
    // Mirrors testBatchModeBidirectional: scalar edges, null edge terminal, unified frontier.
    // Eliot's only onward edge (Ron) is claimed by other rows at the same level; the old code
    // dropped whichever of Eliot/Ron lost the arrival-order race.
    List<Object[]> sourceRows =
        List.of(new Object[] {"Dev", "Eliot"}, new Object[] {"Dan", "Andrew"});
    RelNode source = scannableSource(sourceRows, EMPLOYEE_FIELDS);
    FakeLookupScan lookup =
        new FakeLookupScan(
            cluster,
            traitSet,
            table,
            osIndex,
            rowType(EMPLOYEE_FIELDS),
            employeeDocs(),
            EMPLOYEE_FIELDS,
            UnaryOperator.identity());

    CalciteEnumerableGraphLookup graphLookup =
        new CalciteEnumerableGraphLookup(
            cluster,
            traitSet,
            source,
            lookup,
            "reportsTo",
            null,
            "reportsTo",
            "name",
            "connections",
            "depth",
            3,
            true,
            true,
            true,
            false,
            null);

    List<@Nullable Object> rows = collect(graphLookup.scan());
    assertEquals(1, rows.size(), "batch mode returns a single aggregated row");
    Object[] row = (Object[]) rows.get(0);

    @SuppressWarnings("unchecked")
    List<Object> collectedSourceRows = (List<Object>) row[0];
    assertEquals(2, collectedSourceRows.size(), "all source rows must be collected");

    @SuppressWarnings("unchecked")
    List<Object> bfsResults = (List<Object>) row[1];
    Map<Object, Object> depths = new HashMap<>();
    for (Object result : bfsResults) {
      Object[] resultRow = (Object[]) result;
      depths.put(resultRow[0], resultRow[2]);
    }
    Map<Object, Object> expected = new HashMap<>();
    expected.put("Dev", 0);
    expected.put("Eliot", 0);
    expected.put("Ron", 0);
    expected.put("Andrew", 0);
    expected.put("Dan", 0);
    expected.put("Asya", 1);
    assertEquals(expected, depths, "both Eliot and Ron must be present regardless of row order");
  }

  @Test
  void normalModeRunsBfsPerSourceRowAndNullStartYieldsEmptyResult() {
    List<Object[]> sourceRows =
        List.of(new Object[] {"row-a", "BOS"}, new Object[] {"row-b", null});
    RelNode source = scannableSource(sourceRows, List.of("id", "start"));
    FakeLookupScan lookup =
        new FakeLookupScan(
            cluster,
            traitSet,
            table,
            osIndex,
            rowType(AIRPORT_FIELDS),
            airportDocs(),
            AIRPORT_FIELDS,
            UnaryOperator.identity());

    CalciteEnumerableGraphLookup graphLookup =
        new CalciteEnumerableGraphLookup(
            cluster,
            traitSet,
            source,
            lookup,
            "start",
            null,
            "connects",
            "airport",
            "output",
            null,
            0,
            false,
            true,
            false,
            false,
            null);

    List<@Nullable Object> rows = collect(graphLookup.scan());
    assertEquals(2, rows.size(), "normal mode emits one output row per source row");

    Object[] first = (Object[]) rows.get(0);
    assertEquals("row-a", first[0]);
    @SuppressWarnings("unchecked")
    List<Object> firstResults = (List<Object>) first[2];
    assertEquals(1, firstResults.size());
    assertEquals("BOS", ((Object[]) firstResults.get(0))[0]);

    Object[] second = (Object[]) rows.get(1);
    assertEquals("row-b", second[0]);
    @SuppressWarnings("unchecked")
    List<Object> secondResults = (List<Object>) second[2];
    assertTrue(secondResults.isEmpty(), "a null start value must produce an empty result array");
  }

  // ---------------------------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------------------------

  /** Runs a literal-start-mode BFS and returns the emitted lookup rows. */
  private List<Object[]> runLiteralBfs(
      List<Object[]> docs,
      List<String> fieldNames,
      List<Object> startValues,
      String fromField,
      String toField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional,
      UnaryOperator<List<Object[]>> returnOrder) {
    FakeLookupScan lookup =
        new FakeLookupScan(
            cluster, traitSet, table, osIndex, rowType(fieldNames), docs, fieldNames, returnOrder);
    CalciteEnumerableGraphLookup graphLookup =
        new CalciteEnumerableGraphLookup(
            cluster,
            traitSet,
            mock(RelNode.class),
            lookup,
            null,
            startValues,
            fromField,
            toField,
            "output",
            depthField,
            maxDepth,
            bidirectional,
            true,
            false,
            false,
            null);

    List<@Nullable Object> rows = collect(graphLookup.scan());
    assertEquals(1, rows.size(), "literal start mode returns a single row");
    @SuppressWarnings("unchecked")
    List<Object> bfsResults = (List<Object>) rows.get(0);
    List<Object[]> results = new ArrayList<>();
    for (Object result : bfsResults) {
      results.add((Object[]) result);
    }
    return results;
  }

  private static List<@Nullable Object> collect(Enumerable<@Nullable Object> enumerable) {
    return new ArrayList<>(enumerable.toList());
  }

  /** Maps each emitted row's first column to its depth column (last column). */
  private static Map<Object, Object> depthByKey(List<Object[]> results) {
    Map<Object, Object> depths = new HashMap<>();
    for (Object[] row : results) {
      Object previous = depths.put(row[0], row[row.length - 1]);
      assertEquals(null, previous, "document " + row[0] + " was emitted more than once");
    }
    return depths;
  }

  private static long countAtDepth(List<Object[]> results, int depth) {
    return results.stream()
        .filter(row -> Integer.valueOf(depth).equals(row[row.length - 1]))
        .count();
  }

  private static RelDataType rowType(List<String> fieldNames) {
    RelDataTypeFactory.Builder builder = TYPE_FACTORY.builder();
    for (String name : fieldNames) {
      builder.add(name, TYPE_FACTORY.createSqlType(SqlTypeName.ANY));
    }
    return builder.build();
  }

  /** A Scannable source RelNode returning fixed rows. */
  private RelNode scannableSource(List<Object[]> rows, List<String> fieldNames) {
    RelNode source = mock(RelNode.class, withSettings().extraInterfaces(Scannable.class));
    RelDataType sourceRowType = mock(RelDataType.class);
    lenient().when(sourceRowType.getFieldNames()).thenReturn(fieldNames);
    lenient().when(source.getRowType()).thenReturn(sourceRowType);
    lenient()
        .when(((Scannable) source).scan())
        .thenReturn(Linq4j.asEnumerable(new ArrayList<Object>(rows)));
    return source;
  }

  /**
   * In-memory stand-in for the lookup index scan. {@link #scan()} extracts the terms query the
   * enumerator pushed into {@link #pushDownContext}, evaluates it against the fixed document list,
   * and returns the matches through a configurable order transform (simulating shard return order).
   */
  private static class FakeLookupScan extends CalciteEnumerableIndexScan {
    private final List<Object[]> docs;
    private final List<String> fieldNames;
    private final UnaryOperator<List<Object[]>> returnOrder;

    FakeLookupScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        List<Object[]> docs,
        List<String> fieldNames,
        UnaryOperator<List<Object[]>> returnOrder) {
      super(cluster, traitSet, List.of(), table, osIndex, schema, new PushDownContext(osIndex));
      this.docs = docs;
      this.fieldNames = fieldNames;
      this.returnOrder = returnOrder;
    }

    @Override
    public AbstractCalciteIndexScan copy() {
      FakeLookupScan copy =
          new FakeLookupScan(
              getCluster(),
              getTraitSet(),
              getTable(),
              osIndex,
              schema,
              docs,
              fieldNames,
              returnOrder);
      // Preserve the operations already pushed to this scan, as the real copy() does.
      for (PushDownOperation operation : pushDownContext) {
        copy.pushDownContext.add(operation);
      }
      return copy;
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
      QueryBuilder filter = extractPushedFilter();
      List<Object[]> matches = new ArrayList<>();
      for (Object[] doc : docs) {
        if (matches(filter, doc)) {
          matches.add(doc);
        }
      }
      List<Object[]> ordered = returnOrder.apply(matches);
      return Linq4j.asEnumerable(new ArrayList<@Nullable Object>(ordered));
    }

    /** Replays the pushed FILTER actions against a capturing request builder. */
    private QueryBuilder extractPushedFilter() {
      OpenSearchRequestBuilder requestBuilder = Mockito.mock(OpenSearchRequestBuilder.class);
      for (PushDownOperation operation : pushDownContext) {
        if (operation.type() == PushDownType.FILTER) {
          @SuppressWarnings("unchecked")
          AbstractAction<OpenSearchRequestBuilder> action =
              (AbstractAction<OpenSearchRequestBuilder>) operation.action();
          action.apply(requestBuilder);
        }
      }
      ArgumentCaptor<QueryBuilder> captor = ArgumentCaptor.forClass(QueryBuilder.class);
      Mockito.verify(requestBuilder).pushDownFilterForCalcite(captor.capture());
      return captor.getValue();
    }

    private boolean matches(QueryBuilder query, Object[] doc) {
      if (query instanceof TermsQueryBuilder terms) {
        int fieldIndex = fieldNames.indexOf(terms.fieldName());
        assertTrue(fieldIndex >= 0, "terms query on unknown field: " + terms.fieldName());
        Object cell = doc[fieldIndex];
        Set<Object> values = new HashSet<>(terms.values());
        if (cell instanceof List<?> list) {
          return list.stream().anyMatch(values::contains);
        }
        return cell != null && values.contains(cell);
      }
      if (query instanceof BoolQueryBuilder bool) {
        return bool.should().stream().anyMatch(clause -> matches(clause, doc));
      }
      throw new IllegalStateException("unexpected query shape: " + query);
    }
  }
}
