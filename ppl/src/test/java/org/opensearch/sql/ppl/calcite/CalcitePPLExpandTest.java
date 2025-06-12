/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
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
import org.junit.Test;

public class CalcitePPLExpandTest extends CalcitePPLAbstractTest {

  public CalcitePPLExpandTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  // There is no existing table with arrays. We create one for test purpose.
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
    // Add an empty table with name DEPT for test purpose
    schema.add("DEPT", new TableWithArray());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testExpand() {
    String ppl = "source=DEPT | expand EMPNOS";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMPNOS=[$2])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "    LogicalSystemLimit(fetch=[50000])\n"
            + "      Uncollect\n"
            + "        LogicalProject(EMPNOS=[$cor0.EMPNOS])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `t1`.`EMPNOS`\n"
            + "FROM `scott`.`DEPT` `$cor0`,\n"
            + "LATERAL (SELECT `EMPNOS`\n"
            + "FROM UNNEST (SELECT `$cor0`.`EMPNOS`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`)) `t0` (`EMPNOS`)\n"
            + "LIMIT 50000) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExpandWithEval() {
    String ppl = "source=DEPT | eval employee_no = EMPNOS | expand employee_no";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMPNOS=[$1], employee_no=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])\n"
            + "    LogicalProject(DEPTNO=[$0], EMPNOS=[$1], employee_no=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "    LogicalSystemLimit(fetch=[50000])\n"
            + "      Uncollect\n"
            + "        LogicalProject(employee_no=[$cor0.employee_no])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `$cor0`.`EMPNOS`, `t2`.`employee_no`\n"
            + "FROM (SELECT `DEPTNO`, `EMPNOS`, `EMPNOS` `employee_no`\n"
            + "FROM `scott`.`DEPT`) `$cor0`,\n"
            + "LATERAL (SELECT `employee_no`\n"
            + "FROM UNNEST (SELECT `$cor0`.`employee_no`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`)) `t1` (`employee_no`)\n"
            + "LIMIT 50000) `t2`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
