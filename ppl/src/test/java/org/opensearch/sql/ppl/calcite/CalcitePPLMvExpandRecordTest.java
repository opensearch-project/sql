/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

/**
 * {@code mvexpand} over an array whose elements are records.
 *
 * <p>Uncollect explodes a record element into one column per record field, so the row type after it
 * is the element's field names and the expanded column's own name is gone. The columns are
 * re-wrapped into a single ROW named after the array, which is what keeps {@code <array>.<leaf>}
 * addressable afterwards.
 *
 * <p>This lives in its own class rather than in {@link CalcitePPLMvExpandTest} because adding a
 * record-array column to that shared fixture would appear in every one of its expected plans.
 */
public class CalcitePPLMvExpandRecordTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvExpandRecordTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /** A table with an ARRAY of records, the shape no existing test table has. */
  public static class TableWithRecordArray implements Table {
    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("ID", SqlTypeName.INTEGER)
                .add(
                    "EVENTS",
                    factory.createArrayType(
                        factory
                            .builder()
                            .add("NAME", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("TIME", factory.createSqlType(SqlTypeName.INTEGER))
                            .build(),
                        -1))
                .add(
                    "TAGS", factory.createArrayType(factory.createSqlType(SqlTypeName.VARCHAR), -1))
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
    schema.add("LOGS", new TableWithRecordArray());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * The expanded column survives as one ROW column named after the array. Without the re-wrap the
   * row type carries the element's own field names instead and the lookup of the array's name right
   * after Uncollect fails, so this does not plan at all.
   */
  @Test
  public void testMvExpandOfRecordElementsKeepsTheColumnName() {
    RelNode root = getRelNode("source=LOGS | mvexpand EVENTS");
    verifyLogical(
        root,
        "LogicalProject(ID=[$0], TAGS=[$2], EVENTS=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, LOGS]])\n"
            + "    LogicalProject(EVENTS=[ROW($0, $1)])\n"
            + "      Uncollect\n"
            + "        LogicalProject(EVENTS=[$cor0.EVENTS])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n");
  }

  /**
   * A scalar element needs no re-wrap and must not get one: Uncollect already yields exactly one
   * column carrying the array's name, so this shape keeps working on its existing path.
   */
  @Test
  public void testMvExpandOfScalarElementsIsUnchanged() {
    RelNode root = getRelNode("source=LOGS | mvexpand TAGS");
    verifyLogical(
        root,
        "LogicalProject(ID=[$0], EVENTS=[$1], TAGS=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])\n"
            + "    LogicalTableScan(table=[[scott, LOGS]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(TAGS=[$cor0.TAGS])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n");
  }
}
