/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
  public void testSearchWithImplicitFormatSubsearchUsesCorrelate() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    LogicalCorrelate correlate = findCorrelate(root);
    String logical = RelOptUtil.toString(root);

    Assert.assertTrue(logical, logical.contains("LogicalCorrelate"));
    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("ARRAY_AGG"));
    Assert.assertTrue(logical, logical.contains("query_string"));
    Assert.assertSame(root.getCluster(), correlate.getLeft().getCluster());
    Assert.assertSame(root.getCluster(), correlate.getRight().getCluster());
  }

  @Test
  public void testImplicitFormatQueryStringReferencesCorrelatedValue() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    String logical = RelOptUtil.toString(root);

    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("query_string"));
    Assert.assertTrue(logical, logical.contains("$cor"));
  }

  @Test
  public void testImplicitFormatCombinesWithStaticSearchPredicate() {
    RelNode root =
        getRelNode("search source=EMP JOB=CLERK [ search source=DEPT | fields DEPTNO | head 1 ]");
    String logical = RelOptUtil.toString(root);

    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("JOB:CLERK"));
    Assert.assertTrue(logical, logical.contains("$cor"));
    Assert.assertTrue(logical, logical.contains("AND"));
  }

  @Test
  public void testMultipleImplicitFormatSubsearchesShareOneCorrelate() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ] OR "
                + "[ search source=EMP | fields JOB | head 1 ]");
    LogicalCorrelate correlate = findCorrelate(root);
    String logical = RelOptUtil.toString(root);

    Assert.assertTrue(correlate.getLeft() instanceof LogicalJoin);
    Assert.assertEquals(1, countCorrelates(root));
    Assert.assertFalse(logical, logical.contains("SCALAR_QUERY"));
    Assert.assertTrue(logical, logical.contains("OR"));
  }

  @Test
  public void testImplicitFormatSubsearchProducesSearchPredicateData() {
    RelNode root = getRelNode("search source=EMP [ search source=DEPT | fields DEPTNO | head 1 ]");
    RelNode formatSubquery = findCorrelate(root).getLeft();

    verifyResult(formatSubquery, "search=( ( DEPTNO=\"10\" ) )\n");
  }

  @Test
  public void testImplicitFormatDoesNotExecuteMultivalueSearchFieldAsRawPredicate() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | head 1 "
                + "| eval search=array('DEPTNO=10', 'DEPTNO=20') | fields search ]");
    RelNode formatSubquery = findCorrelate(root).getLeft();

    verifyResult(
        formatSubquery, "search=( ( ( search=\"DEPTNO=10\" OR search=\"DEPTNO=20\" ) ) )\n");
  }

  @Test
  public void testImplicitFormatUsesFirstScalarSearchValueAndIgnoresOtherFields() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT "
                + "| eval search=if(DEPTNO=10, 'DEPTNO=20', 'DEPTNO=10') "
                + "| fields search, DEPTNO ]");
    RelNode formatSubquery = findCorrelate(root).getLeft();

    verifyResult(formatSubquery, "search=DEPTNO=20\n");
  }

  @Test
  public void testImplicitFormatUsesPostFormatEvalResult() {
    RelNode root =
        getRelNode(
            "search source=EMP [ search source=DEPT | where DEPTNO=10 | fields DEPTNO "
                + "| format | eval search=replace(search, '10', '20') ]");
    RelNode formatSubquery = findCorrelate(root).getLeft();

    verifyResult(formatSubquery, "search=( ( DEPTNO=\"20\" ) )\n");
  }

  private LogicalCorrelate findCorrelate(RelNode root) {
    AtomicReference<LogicalCorrelate> result = new AtomicReference<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof LogicalCorrelate correlate) {
          result.compareAndSet(null, correlate);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(root);
    Assert.assertNotNull(RelOptUtil.toString(root), result.get());
    return result.get();
  }

  private int countCorrelates(RelNode root) {
    int[] count = {0};
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof LogicalCorrelate) {
          count[0]++;
        }
        super.visit(node, ordinal, parent);
      }
    }.go(root);
    return count[0];
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
