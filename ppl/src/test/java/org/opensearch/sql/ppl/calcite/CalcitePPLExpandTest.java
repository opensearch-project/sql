/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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

  /** A table with a map column, whose leaves are reached with ITEM rather than a column ref. */
  public static class TableWithMap extends TableWithArray {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory
          .builder()
          .add("DEPTNO", SqlTypeName.INTEGER)
          .add(
              "ATTRS",
              typeFactory.createMapType(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR)))
          .build();
    }
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add an empty table with name DEPT for test purpose
    schema.add("DEPT", new TableWithArray());
    schema.add("DEPT_MAP", new TableWithMap());
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
  public void testExpandWithEval() {
    String ppl = "source=DEPT | eval employee_no = EMPNOS | expand employee_no";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMPNOS=[$1], employee_no=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])\n"
            + "    LogicalProject(DEPTNO=[$0], EMPNOS=[$1], employee_no=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(employee_no=[$cor0.employee_no])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, `$cor0`.`EMPNOS`, `t10`.`employee_no`\n"
            + "FROM (SELECT `DEPTNO`, `EMPNOS`, `EMPNOS` `employee_no`\n"
            + "FROM `scott`.`DEPT`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`employee_no`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t10` (`employee_no`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // expand correlates the expansion with a column of its input. A leaf of a map, or any other
  // value computed from a column, is not one -- it reaches the planner as an ITEM call -- and the
  // cast used to fail as a ClassCastException, which reached the user as an internal error.
  @Test
  public void testExpandOfAValueThatIsNotAColumn() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=DEPT_MAP | expand ATTRS.k"));
    assertFalse(
        "expand of a non-column must not surface as a ClassCastException",
        thrown instanceof ClassCastException
            || thrown.getCause() instanceof ClassCastException
            || String.valueOf(thrown.getMessage()).contains("ClassCastException"));
    assertTrue(
        "the message should name the field: " + thrown.getMessage(),
        String.valueOf(thrown.getMessage()).contains("ATTRS.k"));
  }
}
