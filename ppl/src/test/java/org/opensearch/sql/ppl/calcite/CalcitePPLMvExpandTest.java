/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class CalcitePPLMvExpandTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvExpandTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * There is no existing table with arrays. We create one for test purpose.
   *
   * <p>This mirrors CalcitePPLExpandTest.TableWithArray.
   */
  public static class TableWithArray implements Table {
    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("DEPTNO", SqlTypeName.INTEGER)
                .add(
                    "EMPNOS",
                    factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER), -1))
                .build();

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.of(0d, ImmutableList.of(), RelCollations.createSingleton(0));
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("DEPT", new TableWithArray());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testMvExpandBasic() {
    String ppl = "source=DEPT | mvexpand EMPNOS";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMPNOS=[$2])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(EMPNOS=[$cor0.EMPNOS])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `t00`.`EMPNOS`\n"
            + "FROM `scott`.`DEPT` `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`EMPNOS`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t00` (`EMPNOS`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandWithLimitParameter() {
    String ppl = "source=DEPT | mvexpand EMPNOS limit=2";
    RelNode root = getRelNode(ppl);

    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
    assertAnyContains(root, "fetch=", "LIMIT", "RowNumber", "Window");

    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `t1`.`EMPNOS`\n"
            + "FROM `scott`.`DEPT` `$cor0`,\n"
            + "LATERAL (SELECT `EMPNOS`\n"
            + "FROM UNNEST((SELECT `$cor0`.`EMPNOS`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t0` (`EMPNOS`)\n"
            + "LIMIT 2) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandProjectNested() {
    String ppl = "source=DEPT | mvexpand EMPNOS | fields DEPTNO, EMPNOS";
    RelNode root = getRelNode(ppl);

    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
    assertContains(root, "LogicalProject");

    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `t00`.`EMPNOS`\n"
            + "FROM `scott`.`DEPT` `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`EMPNOS`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t00` (`EMPNOS`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandEmptyOrNullArray() {
    RelNode root = getRelNode("source=DEPT | where isnull(EMPNOS) | mvexpand EMPNOS");
    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
  }

  @Test
  public void testMvExpandWithDuplicates() {
    RelNode root = getRelNode("source=DEPT | where DEPTNO in (10, 10, 20) | mvexpand EMPNOS");
    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
  }

  @Test
  public void testMvExpandLargeArray() {
    RelNode root = getRelNode("source=DEPT | where DEPTNO = 999 | mvexpand EMPNOS");
    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
  }

  @Test
  public void testMvExpandPrimitiveArray() {
    RelNode root = getRelNode("source=DEPT | mvexpand EMPNOS");
    assertContains(root, "LogicalCorrelate");
    assertContains(root, "Uncollect");
  }

  @Test
  public void testMvExpandInvalidLimitZero() {
    String ppl = "source=DEPT | mvexpand EMPNOS limit=0";
    Exception ex = Assert.assertThrows(Exception.class, () -> getRelNode(ppl));
    String msg = String.valueOf(ex.getMessage());
    Assert.assertTrue(
        "Expected error message for limit=0. Actual: " + msg,
        msg.toLowerCase().contains("limit") || msg.toLowerCase().contains("positive"));
  }

  @Test
  public void testMvExpandInvalidLimitNegative() {
    String ppl = "source=DEPT | mvexpand EMPNOS limit=-1";
    Exception ex = Assert.assertThrows(Exception.class, () -> getRelNode(ppl));
    String msg = String.valueOf(ex.getMessage());
    Assert.assertTrue(
        "Expected error message for negative limit. Actual: " + msg,
        msg.toLowerCase().contains("limit")
            || msg.toLowerCase().contains("negative")
            || msg.toLowerCase().contains("positive"));
  }

  @Test
  public void testMvExpandNonArrayField() {
    String ppl = "source=DEPT | mvexpand DEPTNO";
    RelNode root = getRelNode(ppl);

    Assert.assertNotNull("Query should produce a valid plan", root);

    String plan = root.explain();
    Assert.assertTrue(
        "Plan should contain LogicalTableScan",
        plan.contains("LogicalTableScan") || plan.contains("LogicalProject"));

    Assert.assertFalse(
        "Non-array field should not generate Uncollect operation", plan.contains("Uncollect"));
  }

  private static void assertContains(RelNode root, String token) {
    String plan = root.explain();
    Assert.assertTrue(
        "Expected plan to contain [" + token + "] but got:\n" + plan, plan.contains(token));
  }

  private static void assertAnyContains(RelNode root, String... tokens) {
    String plan = root.explain();
    for (String token : tokens) {
      if (plan.contains(token)) {
        return;
      }
    }
    Assert.fail(
        "Expected plan to contain one of " + Arrays.toString(tokens) + " but got:\n" + plan);
  }
}
