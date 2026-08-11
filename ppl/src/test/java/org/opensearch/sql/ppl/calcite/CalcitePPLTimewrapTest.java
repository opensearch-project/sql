/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.util.List;
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
import org.junit.Test;

/**
 * Unit tests for the {@code timewrap} command's Calcite plan construction. Timewrap reshapes {@code
 * timechart} output, so every query pipes through {@code timechart span=...} first. The pivot
 * itself is post-processing in the execution engine (see {@code TimewrapPivot}); these tests cover
 * the {@link org.opensearch.sql.calcite.CalciteRelNodeVisitor#visitTimewrap} lowering — the
 * unpivoted RelNode and its generated Spark SQL.
 */
public class CalcitePPLTimewrapTest extends CalcitePPLAbstractTest {

  public CalcitePPLTimewrapTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {java.sql.Timestamp.valueOf("2024-07-01 00:00:00"), 180},
            new Object[] {java.sql.Timestamp.valueOf("2024-07-01 06:00:00"), 240},
            new Object[] {java.sql.Timestamp.valueOf("2024-07-02 00:00:00"), 205},
            new Object[] {java.sql.Timestamp.valueOf("2024-07-03 00:00:00"), 165});
    schema.add("events", new EventsTable(rows));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  // align=end query with a deterministic WHERE upper bound (2024-07-03 18:00:00 = epoch
  // 1720029600), so the base_offset reference is stable across runs rather than the query clock.
  private static final String TIMEWRAP_DAY =
      "source=events | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
          + " '2024-07-03 18:00:00' | timechart span=6h sum(value) | timewrap 1day align=end";

  @Test
  public void testTimewrapDayProducesUnpivotedPlan() {
    RelNode root = getRelNode(TIMEWRAP_DAY);
    // The pivot is post-processing in the execution engine; the RelNode is intentionally unpivoted:
    // [display_ts, value, __base_offset__, __period__], sorted by (display_ts, period). base_offset
    // uses FLOOR(.../span) (not truncating integer divide) so future-dated align=now stays correct.
    String expectedLogical =
        "LogicalSort(sort0=[$0], sort1=[$3], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalProject(@timestamp=[FROM_UNIXTIME(+(-(MAX(CAST(UNIX_TIMESTAMP($0)):BIGINT"
            + " NOT NULL) OVER (), MOD(MAX(CAST(UNIX_TIMESTAMP($0)):BIGINT NOT NULL) OVER (),"
            + " 86400:BIGINT)), MOD(CAST(UNIX_TIMESTAMP($0)):BIGINT NOT NULL, 86400:BIGINT)))],"
            + " sum(value)=[$1], __base_offset__=[CAST(FLOOR(/(CAST(-(1720029600,"
            + " MAX(CAST(UNIX_TIMESTAMP($0)):BIGINT NOT NULL) OVER ())):DOUBLE NOT NULL,"
            + " 86400:BIGINT))):BIGINT NOT NULL], __period__=[+(/(-(MAX(CAST(UNIX_TIMESTAMP($0))"
            + ":BIGINT NOT NULL) OVER (), CAST(UNIX_TIMESTAMP($0)):BIGINT NOT NULL), 86400), 1)])\n"
            + "    LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "      LogicalProject(@timestamp=[$0], sum(value)=[$1])\n"
            + "        LogicalAggregate(group=[{1}], sum(value)=[CHECKED_LONG_SUM($0)])\n"
            + "          LogicalProject(value=[$1], @timestamp0=[SPAN($0, 6, 'h')])\n"
            + "            LogicalFilter(condition=[AND(>=($0, TIMESTAMP('2024-07-01"
            + " 00:00:00':VARCHAR)), <=($0, TIMESTAMP('2024-07-03 18:00:00':VARCHAR)), IS NOT"
            + " NULL($1))])\n"
            + "              LogicalTableScan(table=[[scott, events]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testTimewrapDaySparkSql() {
    RelNode root = getRelNode(TIMEWRAP_DAY);
    String expectedSparkSql =
        "SELECT FROM_UNIXTIME((MAX(CAST(UNIX_TIMESTAMP(`@timestamp`) AS BIGINT)) OVER (RANGE"
            + " BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) -"
            + " MOD(MAX(CAST(UNIX_TIMESTAMP(`@timestamp`) AS BIGINT)) OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING), 86400) + MOD(CAST(UNIX_TIMESTAMP(`@timestamp`)"
            + " AS BIGINT), 86400)) `@timestamp`, `sum(value)`, CAST(FLOOR(CAST(1720029600 -"
            + " (MAX(CAST(UNIX_TIMESTAMP(`@timestamp`) AS BIGINT)) OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING)) AS DOUBLE) / 86400) AS BIGINT)"
            + " `__base_offset__`, ((MAX(CAST(UNIX_TIMESTAMP(`@timestamp`) AS BIGINT)) OVER (RANGE"
            + " BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) -"
            + " CAST(UNIX_TIMESTAMP(`@timestamp`) AS BIGINT)) / 86400 + 1 `__period__`\n"
            + "FROM (SELECT SPAN(`@timestamp`, 6, 'h') `@timestamp`,"
            + " SUM(`value`) `sum(value)`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` >= TIMESTAMP('2024-07-01 00:00:00') AND `@timestamp` <="
            + " TIMESTAMP('2024-07-03 18:00:00') AND `value` IS NOT NULL\n"
            + "GROUP BY SPAN(`@timestamp`, 6, 'h')\n"
            + "ORDER BY 1 NULLS LAST) `t3`\n"
            + "ORDER BY 1 NULLS LAST, 4 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Minimal time-series table: a nullable timestamp and an integer measure. */
  public static class EventsTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    public EventsTable(ImmutableList<Object[]> rows) {
      this.rows = rows;
    }

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("@timestamp", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("value", SqlTypeName.INTEGER)
                .nullable(true)
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
