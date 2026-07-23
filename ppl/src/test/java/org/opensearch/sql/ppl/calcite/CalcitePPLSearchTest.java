/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.calcite.DynamicSearchPlanBinder;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.DynamicSearchExecutor;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.ppl.parser.PPLSearchPredicateCompiler;

public class CalcitePPLSearchTest extends CalcitePPLAbstractTest {
  public CalcitePPLSearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    schema.add(
        "LOGS",
        new CalcitePPLEarliestLatestTestUtils.LogsTable(
            CalcitePPLEarliestLatestTestUtils.createLogsTestData()));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testSearchWithFilter() {
    String ppl = "search source=EMP DEPTNO=20";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[query_string(MAP('query', 'DEPTNO:20':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\nFROM `scott`.`EMP`\nWHERE QUERY_STRING(MAP ('query', 'DEPTNO:20'))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSearchWithImplicitFormatSubsearchUsesScalarQuery() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    RelNode subquery = DynamicSearchPlanBinder.find(root).orElseThrow().rel;
    String logical = RelOptUtil.toString(root);
    Assert.assertTrue(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("ARRAY_AGG"));
    Assert.assertTrue(logical, logical.contains("query_string"));
    Assert.assertNotSame(root.getCluster(), subquery.getCluster());
  }

  @Test
  public void testBoundImplicitFormatHasSameStaticQueryStringShape() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    var subquery = DynamicSearchPlanBinder.find(root).orElseThrow();

    RelNode bound = DynamicSearchPlanBinder.bind(root, subquery, "DEPTNO:10");
    String logical = RelOptUtil.toString(bound);

    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("query_string(MAP('query', 'DEPTNO:10':VARCHAR))"));
  }

  @Test
  public void testBoundImplicitFormatCombinesWithStaticSearchPredicate() {
    RelNode root =
        getRelNode("search source=EMP JOB=CLERK [ search source=DEPT | fields DEPTNO | head 1 ]");
    var subquery = DynamicSearchPlanBinder.find(root).orElseThrow();

    RelNode bound = DynamicSearchPlanBinder.bind(root, subquery, "DEPTNO:10");
    String logical = RelOptUtil.toString(bound);

    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(
        logical,
        logical.contains("query_string(MAP('query', '(JOB:CLERK) AND (DEPTNO:10)':VARCHAR))"));
  }

  @Test
  public void testMultipleImplicitFormatSubsearchesBindSequentially() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ] OR "
                + "[ search source=EMP | fields JOB | head 1 ]");

    RelNode firstBound =
        DynamicSearchPlanBinder.bind(
            root, DynamicSearchPlanBinder.find(root).orElseThrow(), "DEPTNO:10");
    Assert.assertTrue(DynamicSearchPlanBinder.find(firstBound).isPresent());
    RelNode fullyBound =
        DynamicSearchPlanBinder.bind(
            firstBound, DynamicSearchPlanBinder.find(firstBound).orElseThrow(), "JOB:CLERK");
    String logical = RelOptUtil.toString(fullyBound);

    Assert.assertTrue(DynamicSearchPlanBinder.find(fullyBound).isEmpty());
    Assert.assertTrue(
        logical,
        logical.contains("query_string(MAP('query', '(DEPTNO:10 OR JOB:CLERK)':VARCHAR))"));
  }

  @Test
  public void testImplicitFormatSubsearchProducesSearchPredicateData() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    RelNode formatSubquery = DynamicSearchPlanBinder.find(root).orElseThrow().rel;

    verifyResult(formatSubquery, "search=( ( DEPTNO=\"10\" ) )\n");
  }

  @Test
  public void testImplicitFormatDoesNotExecuteMultivalueSearchFieldAsRawPredicate() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | head 1 "
                + "| eval search=array('DEPTNO=10', 'DEPTNO=20') | fields search ]");
    RelNode formatSubquery = DynamicSearchPlanBinder.find(root).orElseThrow().rel;

    verifyResult(formatSubquery, "search=( ( ( \"DEPTNO=10\" OR \"DEPTNO=20\" ) ) )\n");
  }

  @Test
  public void testImplicitFormatUsesFirstScalarSearchValueAndIgnoresOtherFields() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT "
                + "| eval search=if(DEPTNO=10, 'DEPTNO=20', 'DEPTNO=10') "
                + "| fields search, DEPTNO ]");
    RelNode formatSubquery = DynamicSearchPlanBinder.find(root).orElseThrow().rel;

    verifyResult(formatSubquery, "search=DEPTNO=20\n");
  }

  @Test
  public void testImplicitFormatUsesPostFormatEvalResult() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | where DEPTNO=10 | fields DEPTNO "
                + "| format | eval search=replace(search, '10', '20') ]");
    RelNode formatSubquery = DynamicSearchPlanBinder.find(root).orElseThrow().rel;

    verifyResult(formatSubquery, "search=( ( DEPTNO=\"20\" ) )\n");
  }

  @Test
  public void testImplicitFormatExecutesThenBindsParentSearch() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    AtomicReference<RelNode> executedParent = new AtomicReference<>();
    AtomicReference<Exception> failure = new AtomicReference<>();

    DynamicSearchExecutor.execute(
        root,
        new PPLSearchPredicateCompiler(),
        (subquery, listener) -> executeScalarSubquery(subquery, listener),
        (bound, listener) -> {
          executedParent.set(bound);
          listener.onResponse(
              new ExecutionEngine.QueryResponse(null, Collections.emptyList(), Cursor.None));
        },
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {}

          @Override
          public void onFailure(Exception e) {
            failure.set(e);
          }
        });

    Assert.assertNull(failure.get());
    String logical = RelOptUtil.toString(executedParent.get());
    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(
        logical, logical.contains("query_string(MAP('query', '((DEPTNO:10))':VARCHAR))"));
  }

  private void executeScalarSubquery(
      RelNode subquery, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try (PreparedStatement statement = RelRunners.run(subquery);
        var resultSet = statement.executeQuery()) {
      Assert.assertTrue(resultSet.next());
      listener.onResponse(
          new ExecutionEngine.QueryResponse(
              null,
              List.of(ExprValueUtils.tupleValue(Map.of("search", resultSet.getString(1)))),
              Cursor.None));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  @Ignore("Fields used in search commands are not validated. Enable after fixing it.")
  @Test
  public void testSearchWithoutTimestampShouldThrow() {
    String ppl = "source=EMP earliest='2020-10-11'";
    Throwable t = assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(t, "field [@timestamp] not found");
  }

  @Test
  public void testSearchWithAbsoluteTimeRange() {
    String ppl = "source=LOGS earliest='2020-10-11' latest='2025-01-01'";
    RelNode root = getRelNode(ppl);
    // @timestamp is a field of type DATE here

    String expectedLogical =
        "LogicalFilter(condition=[query_string(MAP('query',"
            + " '(@timestamp:>=2020\\-10\\-11T00\\:00\\:00Z) AND"
            + " (@timestamp:<=2025\\-01\\-01T00\\:00\\:00Z)':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`LOGS`\n"
            + "WHERE QUERY_STRING(MAP ('query', '(@timestamp:>=2020\\-10\\-11T00\\:00\\:00Z) AND"
            + " (@timestamp:<=2025\\-01\\-01T00\\:00\\:00Z)'))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSearchWithTimeSnap() {
    String ppl = "search source=LOGS earliest=@year";
    getRelNode(ppl);
  }
}
