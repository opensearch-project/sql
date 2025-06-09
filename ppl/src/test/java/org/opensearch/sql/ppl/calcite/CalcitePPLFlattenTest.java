/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl.calcite;

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
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
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

/** Unit tests for {@code flatten} command in PPL. */
public class CalcitePPLFlattenTest extends CalcitePPLAbstractTest {
  public CalcitePPLFlattenTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add an empty table with name DEPT for test purpose
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {10, ImmutableList.of(7369, "ALLEN"), "SMITH", 7369},
            new Object[] {20, ImmutableList.of(7499, "ALLEN"), "ALLEN", 7499},
            new Object[] {30, ImmutableList.of(7521, "WARD"), "WARD", 7521});
    schema.add("DEPT", new TableWithStruct(rows));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testFlatten() {
    String ppl = "source=DEPT | flatten EMP";
    RelNode root = getRelNode(ppl);
    // Regarded as an identity scan. See RelBuilder#L2801
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMP=[$1], EMPNAME=[$2], EMPNO=[$3])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT `DEPTNO`, `EMP`, `EMP.EMPNAME` `EMPNAME`, `EMP.EMPNO` `EMPNO`\nFROM `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    String expectedResult =
        "DEPTNO=10; EMP={7369, ALLEN}; EMPNAME=SMITH; EMPNO=7369\n"
            + "DEPTNO=20; EMP={7499, ALLEN}; EMPNAME=ALLEN; EMPNO=7499\n"
            + "DEPTNO=30; EMP={7521, WARD}; EMPNAME=WARD; EMPNO=7521\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testFlattenWithAliases() {
    String ppl = "source=DEPT | flatten EMP as name, number";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMP=[$1], name=[$2], number=[$3])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT `DEPTNO`, `EMP`, `EMP.EMPNAME` `name`, `EMP.EMPNO` `number`\nFROM `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    String expectedResult =
        "DEPTNO=10; EMP={7369, ALLEN}; name=SMITH; number=7369\n"
            + "DEPTNO=20; EMP={7499, ALLEN}; name=ALLEN; number=7499\n"
            + "DEPTNO=30; EMP={7521, WARD}; name=WARD; number=7521\n";
    verifyResult(root, expectedResult);
  }

  /**
   * This validates that the created table is scannable and the nested fields are removed from the
   * result.
   */
  @Test
  public void testProject() {
    String ppl = "source=DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], EMP=[$1])\n  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=10; EMP={7369, ALLEN}\n"
            + "DEPTNO=20; EMP={7499, ALLEN}\n"
            + "DEPTNO=30; EMP={7521, WARD}\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testFlattenWithMismatchedNumberOfAliasesShouldThrow() {
    String ppl = "source=DEPT | flatten EMP as name";
    Throwable t = Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "The number of aliases has to match the number of flattened fields. Expected 2"
            + " (EMP.EMPNAME, EMP.EMPNO), got 1 (name)");
  }

  // There is no existing table with arrays. We create one for test purpose.
  @RequiredArgsConstructor
  public static class TableWithStruct implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("DEPTNO", SqlTypeName.INTEGER)
                .add(
                    "EMP",
                    factory.createStructType(
                        List.of(
                            factory.createSqlType(SqlTypeName.INTEGER),
                            factory.createSqlType(SqlTypeName.VARCHAR)),
                        List.of("EMPNO", "EMPNAME")))
                // structs and objects are read in a flattened manner in OpenSearch
                // E.g. struct emp will always hava emp.empno and emp.empname in its
                // logical projection. We add these two fields to simulate this behavior
                // in opensearch.
                .add("EMP.EMPNAME", SqlTypeName.VARCHAR)
                .add("EMP.EMPNO", SqlTypeName.INTEGER)
                .build();

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
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
