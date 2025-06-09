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
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@code flatten} command in PPL.
 */
public class CalcitePPLFlattenTest extends CalcitePPLAbstractTest {
  public CalcitePPLFlattenTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add an empty table with name DEPT for test purpose
    schema.add("DEPT", new TableWithStruct());
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
    String expectedLogical = "LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT *\nFROM `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFlattenWithAliases() {
    String ppl = "source=DEPT | flatten EMP as name, number";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT *\nFROM `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFlattenWithMismatchedNumberOfAliasesShouldThrow() {
    String ppl = "source=DEPT | flatten EMP as name";
    Throwable t = Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "The number of aliases has to match the number of flattened fields. Expected 2 (EMP.EMPNO,"
            + " EMP.EMPNAME), got 1 (name)");
  }

  // There is no existing table with arrays. We create one for test purpose.
  public static class TableWithStruct implements Table {
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
                .add("EMP.EMPNO", SqlTypeName.INTEGER)
                .add("EMP.EMPNAME", SqlTypeName.VARCHAR)
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
}
