/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/**
 * Calcite tests for the mvexpand command.
 *
 * <p>Planner tests for mvexpand; kept minimal and consistent with other Calcite planner tests.
 *
 * <p>NOTE: - Updated expected Spark-SQL strings to match the new Calcite -> Spark SQL translation
 * emitted by the current CalciteRelNodeVisitor implementation (uses UNNEST subquery form).
 */
public class CalcitePPLMvExpandTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvExpandTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    // Keep dataset empty: tests only need schema/type information.
    ImmutableList<Object[]> users = ImmutableList.of();

    schema.add("USERS", new UsersTable(users));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testMvExpandBasic() {
    String ppl = "source=USERS | mvexpand skills";
    RelNode root;
    try {
      root = getRelNode(ppl);
      // Ensure planner didn't throw and returned a plan
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand basic planning should not throw, but got: " + e.getMessage());
      return;
    }

    String expectedLogical =
        "LogicalProject(USERNAME=[$0], skills.name=[$2])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, USERS]])\n"
            + "    LogicalProject(skills.name=[$0])\n"
            + "      Uncollect\n"
            + "        LogicalProject(skills=[$cor0.skills])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    // Updated expectation: Calcite's current Spark SQL translator emits an UNNEST-style lateral
    // subquery rather than a "LATERAL VIEW EXPLODE(...)" expression. Match that output.
    String expectedSparkSql =
        "SELECT `$cor0`.`USERNAME`, `t1`.`skills.name`\n"
            + "FROM `scott`.`USERS` `$cor0`,\n"
            + "LATERAL (SELECT `name` `skills.name`\n"
            + "FROM UNNEST((SELECT `$cor0`.`skills`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t0` (`name`, `level`)) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandWithLimit() {
    String ppl = "source=USERS | mvexpand skills | head 1";
    RelNode root;
    try {
      root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand with limit planning should not throw, but got: " + e.getMessage());
      return;
    }

    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(USERNAME=[$0], skills.name=[$2])\n"
            + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "      LogicalTableScan(table=[[scott, USERS]])\n"
            + "      LogicalProject(skills.name=[$0])\n"
            + "        Uncollect\n"
            + "          LogicalProject(skills=[$cor0.skills])\n"
            + "            LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    // Same UNNEST-style translation with LIMIT appended
    String expectedSparkSql =
        "SELECT `$cor0`.`USERNAME`, `t1`.`skills.name`\n"
            + "FROM `scott`.`USERS` `$cor0`,\n"
            + "LATERAL (SELECT `name` `skills.name`\n"
            + "FROM UNNEST((SELECT `$cor0`.`skills`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t0` (`name`, `level`)) `t1`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandProjectNested() {
    String ppl = "source=USERS | mvexpand skills | fields USERNAME, skills.name";
    RelNode root;
    try {
      root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand project nested planning should not throw, but got: " + e.getMessage());
      return;
    }

    String expectedLogical =
        "LogicalProject(USERNAME=[$0], skills.name=[$2])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, USERS]])\n"
            + "    LogicalProject(skills.name=[$0])\n"
            + "      Uncollect\n"
            + "        LogicalProject(skills=[$cor0.skills])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`USERNAME`, `t1`.`skills.name`\n"
            + "FROM `scott`.`USERS` `$cor0`,\n"
            + "LATERAL (SELECT `name` `skills.name`\n"
            + "FROM UNNEST((SELECT `$cor0`.`skills`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t0` (`name`, `level`)) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvExpandEmptyOrNullArray() {
    String ppl = "source=USERS | where USERNAME in ('empty','nullskills') | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on empty/null array should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandNoArrayField() {
    String ppl = "source=USERS | where USERNAME = 'noskills' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on missing array field should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandWithDuplicates() {
    String ppl = "source=USERS | where USERNAME = 'duplicate' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand with duplicates should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandLargeArray() {
    String ppl = "source=USERS | where USERNAME = 'large' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on large array should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandPrimitiveArray() {
    String ppl = "source=USERS | where USERNAME = 'primitive' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of primitives should not throw, but got: " + e.getMessage());
    }
  }

  @RequiredArgsConstructor
  static class UsersTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("USERNAME", SqlTypeName.VARCHAR)
                .add(
                    "skills",
                    factory.createArrayType(
                        factory
                            .builder()
                            .add("name", SqlTypeName.VARCHAR)
                            .add("level", SqlTypeName.VARCHAR)
                            .build(),
                        -1))
                .build();

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

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
}
