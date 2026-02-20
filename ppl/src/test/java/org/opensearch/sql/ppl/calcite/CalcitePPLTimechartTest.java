/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

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
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class CalcitePPLTimechartTest extends CalcitePPLAbstractTest {

  public CalcitePPLTimechartTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add events table for timechart tests
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {
              java.sql.Timestamp.valueOf("2024-07-01 00:00:00"),
              java.sql.Timestamp.valueOf("2024-01-15 10:00:00"),
              "web-01",
              "us-east",
              45.2,
              120
            },
            new Object[] {
              java.sql.Timestamp.valueOf("2024-07-01 00:01:00"),
              java.sql.Timestamp.valueOf("2024-02-20 11:00:00"),
              "web-02",
              "us-west",
              38.7,
              150
            },
            new Object[] {
              java.sql.Timestamp.valueOf("2024-07-01 00:02:00"),
              java.sql.Timestamp.valueOf("2024-03-25 12:00:00"),
              "web-01",
              "us-east",
              55.3,
              200
            });
    schema.add("events", new EventsTable(rows));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testTimechartBasic() {
    String ppl = "source=events | timechart count()";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
            + "ORDER BY 1 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartPerSecond() {
    withPPLQuery("source=events | timechart per_second(cpu_usage)")
        .expectSparkSQL(
            "SELECT `@timestamp`, DIVIDE(`per_second(cpu_usage)` * 1.0000E3,"
                + " TIMESTAMPDIFF('MILLISECOND', `@timestamp`, TIMESTAMPADD('MINUTE', 1,"
                + " `@timestamp`))) `per_second(cpu_usage)`\n"
                + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, SUM(`cpu_usage`)"
                + " `per_second(cpu_usage)`\n"
                + "FROM `scott`.`events`\n"
                + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
                + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
                + "ORDER BY 1 NULLS LAST) `t3`");
  }

  @Test
  public void testTimechartPerMinute() {
    withPPLQuery("source=events | timechart per_minute(cpu_usage)")
        .expectSparkSQL(
            "SELECT `@timestamp`, DIVIDE(`per_minute(cpu_usage)` * 6.00000E4,"
                + " TIMESTAMPDIFF('MILLISECOND', `@timestamp`, TIMESTAMPADD('MINUTE', 1,"
                + " `@timestamp`))) `per_minute(cpu_usage)`\n"
                + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, SUM(`cpu_usage`)"
                + " `per_minute(cpu_usage)`\n"
                + "FROM `scott`.`events`\n"
                + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
                + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
                + "ORDER BY 1 NULLS LAST) `t3`");
  }

  @Test
  public void testTimechartPerHour() {
    withPPLQuery("source=events | timechart per_hour(cpu_usage)")
        .expectSparkSQL(
            "SELECT `@timestamp`, DIVIDE(`per_hour(cpu_usage)` * 3.6000000E6,"
                + " TIMESTAMPDIFF('MILLISECOND', `@timestamp`, TIMESTAMPADD('MINUTE', 1,"
                + " `@timestamp`))) `per_hour(cpu_usage)`\n"
                + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, SUM(`cpu_usage`)"
                + " `per_hour(cpu_usage)`\n"
                + "FROM `scott`.`events`\n"
                + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
                + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
                + "ORDER BY 1 NULLS LAST) `t3`");
  }

  @Test
  public void testTimechartPerDay() {
    withPPLQuery("source=events | timechart per_day(cpu_usage)")
        .expectSparkSQL(
            "SELECT `@timestamp`, DIVIDE(`per_day(cpu_usage)` * 8.64E7,"
                + " TIMESTAMPDIFF('MILLISECOND', `@timestamp`, TIMESTAMPADD('MINUTE', 1,"
                + " `@timestamp`))) `per_day(cpu_usage)`\n"
                + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, SUM(`cpu_usage`)"
                + " `per_day(cpu_usage)`\n"
                + "FROM `scott`.`events`\n"
                + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
                + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
                + "ORDER BY 1 NULLS LAST) `t3`");
  }

  @Test
  public void testTimechartWithSpan() {
    String ppl = "source=events | timechart span=1h count()";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT SPAN(`@timestamp`, 1, 'h') `@timestamp`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY SPAN(`@timestamp`, 1, 'h')\n"
            + "ORDER BY 1 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimit() {
    String ppl = "source=events | timechart limit=3 count() by host";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 3 THEN `t2`.`host` ELSE 'OTHER' END `host`,"
            + " SUM(`t2`.`count()`) `count()`\n"
            + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, `host`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'm')) `t2`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`count()`) `__grand_total__`, ROW_NUMBER() OVER (ORDER"
            + " BY SUM(`count()`) DESC) `_row_number_chart_`\n"
            + "FROM (SELECT `host`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'm')) `t6`\n"
            + "WHERE `host` IS NOT NULL\n"
            + "GROUP BY `host`) `t9` ON `t2`.`host` = `t9`.`host`\n"
            + "GROUP BY `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 3 THEN `t2`.`host` ELSE 'OTHER' END\n"
            + "ORDER BY `t2`.`@timestamp` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1h() {
    String ppl = "source=events | timechart span=1h count() by host";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 10 THEN `t2`.`host` ELSE 'OTHER' END `host`,"
            + " SUM(`t2`.`count()`) `count()`\n"
            + "FROM (SELECT SPAN(`@timestamp`, 1, 'h') `@timestamp`, `host`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'h')) `t2`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`count()`) `__grand_total__`, ROW_NUMBER() OVER (ORDER"
            + " BY SUM(`count()`) DESC) `_row_number_chart_`\n"
            + "FROM (SELECT `host`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'h')) `t6`\n"
            + "WHERE `host` IS NOT NULL\n"
            + "GROUP BY `host`) `t9` ON `t2`.`host` = `t9`.`host`\n"
            + "GROUP BY `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 10 THEN `t2`.`host` ELSE 'OTHER' END\n"
            + "ORDER BY `t2`.`@timestamp` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1m() {
    String ppl = "source=events | timechart span=1m avg(cpu_usage) by region";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t2`.`@timestamp`, CASE WHEN `t2`.`region` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 10 THEN `t2`.`region` ELSE 'OTHER' END `region`,"
            + " AVG(`t2`.`avg(cpu_usage)`) `avg(cpu_usage)`\n"
            + "FROM (SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, `region`, AVG(`cpu_usage`)"
            + " `avg(cpu_usage)`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
            + "GROUP BY `region`, SPAN(`@timestamp`, 1, 'm')) `t2`\n"
            + "LEFT JOIN (SELECT `region`, SUM(`avg(cpu_usage)`) `__grand_total__`, ROW_NUMBER()"
            + " OVER (ORDER BY SUM(`avg(cpu_usage)`) DESC) `_row_number_chart_`\n"
            + "FROM (SELECT `region`, AVG(`cpu_usage`) `avg(cpu_usage)`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
            + "GROUP BY `region`, SPAN(`@timestamp`, 1, 'm')) `t6`\n"
            + "WHERE `region` IS NOT NULL\n"
            + "GROUP BY `region`) `t9` ON `t2`.`region` = `t9`.`region`\n"
            + "GROUP BY `t2`.`@timestamp`, CASE WHEN `t2`.`region` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 10 THEN `t2`.`region` ELSE 'OTHER' END\n"
            + "ORDER BY `t2`.`@timestamp` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimitAndUseOtherTrue() {
    String ppl = "source=events | timechart span=1h limit=5 useother=true count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithLimitAndUseOtherFalse() {
    String ppl = "source=events | timechart span=1h limit=3 useother=false avg(cpu_usage) by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 3 THEN `t2`.`host` ELSE 'OTHER' END `host`,"
            + " AVG(`t2`.`avg(cpu_usage)`) `avg(cpu_usage)`\n"
            + "FROM (SELECT SPAN(`@timestamp`, 1, 'h') `@timestamp`, `host`, AVG(`cpu_usage`)"
            + " `avg(cpu_usage)`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'h')) `t2`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`avg(cpu_usage)`) `__grand_total__`, ROW_NUMBER() OVER"
            + " (ORDER BY SUM(`avg(cpu_usage)`) DESC) `_row_number_chart_`\n"
            + "FROM (SELECT `host`, AVG(`cpu_usage`) `avg(cpu_usage)`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL AND `cpu_usage` IS NOT NULL\n"
            + "GROUP BY `host`, SPAN(`@timestamp`, 1, 'h')) `t6`\n"
            + "WHERE `host` IS NOT NULL\n"
            + "GROUP BY `host`) `t9` ON `t2`.`host` = `t9`.`host`\n"
            + "WHERE `t9`.`_row_number_chart_` <= 3\n"
            + "GROUP BY `t2`.`@timestamp`, CASE WHEN `t2`.`host` IS NULL THEN 'NULL' WHEN"
            + " `t9`.`_row_number_chart_` <= 3 THEN `t2`.`host` ELSE 'OTHER' END\n"
            + "ORDER BY `t2`.`@timestamp` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimitAndUseOtherT() {
    String ppl = "source=events | timechart span=1h limit=2 useother=t count() by region";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithLimitAndUseOtherF() {
    String ppl =
        "source=events | timechart span=1h limit=4 useother=f avg(response_time) by service";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithParametersInDifferentOrder1() {
    String ppl = "source=events | timechart limit=5 span=1h count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithParametersInDifferentOrder2() {
    String ppl = "source=events | timechart useother=false limit=3 span=1h avg(cpu_usage) by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithParametersInDifferentOrder3() {
    String ppl = "source=events | timechart useother=true span=1h limit=2 count() by region";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithParametersInDifferentOrder4() {
    String ppl =
        "source=events | timechart limit=4 useother=false span=1h avg(response_time) by service";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithLimitBeforeSpan() {
    String ppl = "source=events | timechart limit=5 span=1h count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithUseOtherBeforeSpan() {
    String ppl = "source=events | timechart useother=false span=1h count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithUseOtherBeforeLimit() {
    String ppl = "source=events | timechart useother=true limit=3 count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartUsingZeroSpanShouldThrow() {
    String ppl = "source=events | timechart span=0h limit=5 count() by host";
    Throwable t = assertThrows(IllegalArgumentException.class, () -> parsePPL(ppl));
    verifyErrorMessageContains(t, "Zero or negative time interval not supported: 0h");
  }

  // ==================== Timechart with Reverse tests ====================
  // These tests verify that reverse works correctly with timechart.
  // Timechart always adds a sort at the end of its plan, so reverse will
  // find the collation via metadata query (tier 1) and flip the sort direction.

  @Test
  public void testTimechartWithReverse() {
    // Timechart adds ORDER BY @timestamp ASC at the end
    // Reverse should flip it to DESC
    String ppl = "source=events | timechart count() | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the timechart's ASC sort in-place with DESC
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "  LogicalProject(@timestamp=[$0], count()=[$1])\n"
            + "    LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "      LogicalProject(@timestamp0=[SPAN($0, 1, 'm')])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($0)])\n"
            + "          LogicalTableScan(table=[[scott, events]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT SPAN(`@timestamp`, 1, 'm') `@timestamp`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `@timestamp` IS NOT NULL\n"
            + "GROUP BY SPAN(`@timestamp`, 1, 'm')\n"
            + "ORDER BY 1 DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithCustomTimefieldAndReverse() {
    // Timechart with custom timefield should also work with reverse
    // The sort is on created_at (the custom field), not @timestamp
    String ppl = "source=events | timechart timefield=created_at span=1month count() | reverse";
    RelNode root = getRelNode(ppl);

    // Reverse replaces the timechart's ASC sort in-place with DESC
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "  LogicalProject(created_at=[$0], count()=[$1])\n"
            + "    LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "      LogicalProject(created_at0=[SPAN($1, 1, 'M')])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "          LogicalTableScan(table=[[scott, events]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT SPAN(`created_at`, 1, 'M') `created_at`, COUNT(*) `count()`\n"
            + "FROM `scott`.`events`\n"
            + "WHERE `created_at` IS NOT NULL\n"
            + "GROUP BY SPAN(`created_at`, 1, 'M')\n"
            + "ORDER BY 1 DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  private UnresolvedPlan parsePPL(String query) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    AstBuilder astBuilder = new AstBuilder(query);
    return astBuilder.visit(parser.parse(query));
  }

  @RequiredArgsConstructor
  public static class EventsTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("@timestamp", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("created_at", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("host", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("region", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("cpu_usage", SqlTypeName.DOUBLE)
                .nullable(true)
                .add("response_time", SqlTypeName.INTEGER)
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
