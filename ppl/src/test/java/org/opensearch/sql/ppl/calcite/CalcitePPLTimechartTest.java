/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;

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
              java.sql.Timestamp.valueOf("2024-07-01 00:00:00"), "web-01", "us-east", 45.2, 120
            },
            new Object[] {
              java.sql.Timestamp.valueOf("2024-07-01 00:01:00"), "web-02", "us-west", 38.7, 150
            },
            new Object[] {
              java.sql.Timestamp.valueOf("2024-07-01 00:02:00"), "web-01", "us-east", 55.3, 200
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
        "SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, COUNT(*) `count`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `SPAN`(`@timestamp`, 1, 'm')\n"
            + "ORDER BY 1 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan() {
    String ppl = "source=events | timechart span=1h count()";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `SPAN`(`@timestamp`, 1, 'h') `@timestamp`, COUNT(*) `count`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `SPAN`(`@timestamp`, 1, 'h')\n"
            + "ORDER BY 1 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithBy() {
    String ppl = "source=events | timechart count() by host";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `@timestamp`, `host`, SUM(`actual_count`) `count`\n"
            + "FROM (SELECT `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host`"
            + " ELSE 'OTHER' END `host`, SUM(`t1`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t1`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t4`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t6` ON `t1`.`host` IS NOT DISTINCT FROM `t6`.`host`\n"
            + "GROUP BY `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE"
            + " 'OTHER' END\n"
            + "UNION\n"
            + "SELECT `t12`.`@timestamp`, `t22`.`$f0` `host`, 0 `count`\n"
            + "FROM (SELECT `@timestamp`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t11`\n"
            + "GROUP BY `@timestamp`) `t12`\n"
            + "CROSS JOIN (SELECT CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING) `$f0`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t15`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t18`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t20` ON `t15`.`host` IS NOT DISTINCT FROM `t20`.`host`\n"
            + "GROUP BY CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING)) `t22`) `t24`\n"
            + "GROUP BY `@timestamp`, `host`\n"
            + "ORDER BY `@timestamp` NULLS LAST, `host` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimit() {
    String ppl = "source=events | timechart limit=3 count() by host";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `@timestamp`, `host`, SUM(`actual_count`) `count`\n"
            + "FROM (SELECT `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host`"
            + " ELSE 'OTHER' END `host`, SUM(`t1`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t1`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t4`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t6` ON `t1`.`host` IS NOT DISTINCT FROM `t6`.`host`\n"
            + "GROUP BY `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE"
            + " 'OTHER' END\n"
            + "UNION\n"
            + "SELECT `t12`.`@timestamp`, `t22`.`$f0` `host`, 0 `count`\n"
            + "FROM (SELECT `@timestamp`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t11`\n"
            + "GROUP BY `@timestamp`) `t12`\n"
            + "CROSS JOIN (SELECT CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING) `$f0`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t15`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t18`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t20` ON `t15`.`host` IS NOT DISTINCT FROM `t20`.`host`\n"
            + "GROUP BY CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING)) `t22`) `t24`\n"
            + "GROUP BY `@timestamp`, `host`\n"
            + "ORDER BY `@timestamp` NULLS LAST, `host` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1h() {
    String ppl = "source=events | timechart span=1h count() by host";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `@timestamp`, `host`, SUM(`actual_count`) `count`\n"
            + "FROM (SELECT `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host`"
            + " ELSE 'OTHER' END `host`, SUM(`t1`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'h') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t1`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t4`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t6` ON `t1`.`host` IS NOT DISTINCT FROM `t6`.`host`\n"
            + "GROUP BY `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE"
            + " 'OTHER' END\n"
            + "UNION\n"
            + "SELECT `t12`.`@timestamp`, `t22`.`$f0` `host`, 0 `count`\n"
            + "FROM (SELECT `@timestamp`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'h') `@timestamp`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t11`\n"
            + "GROUP BY `@timestamp`) `t12`\n"
            + "CROSS JOIN (SELECT CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING) `$f0`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'h') `@timestamp`, `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t15`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t18`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t20` ON `t15`.`host` IS NOT DISTINCT FROM `t20`.`host`\n"
            + "GROUP BY CAST(CASE WHEN `t20`.`host` IS NOT NULL THEN `t15`.`host` ELSE 'OTHER' END AS STRING)) `t22`) `t24`\n"
            + "GROUP BY `@timestamp`, `host`\n"
            + "ORDER BY `@timestamp` NULLS LAST, `host` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1m() {
    String ppl = "source=events | timechart span=1m avg(cpu_usage) by region";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`@timestamp`, CASE WHEN `t6`.`region` IS NOT NULL THEN `t1`.`region` ELSE"
            + " 'OTHER' END `region`, SUM(`t1`.`$f2`) `avg(cpu_usage)`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'm') `@timestamp`, `region`, AVG(`cpu_usage`)"
            + " `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `region`, `SPAN`(`@timestamp`, 1, 'm')) `t1`\n"
            + "LEFT JOIN (SELECT `region`, SUM(`$f2`) `grand_total`\n"
            + "FROM (SELECT `region`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `region`, `SPAN`(`@timestamp`, 1, 'm')) `t4`\n"
            + "GROUP BY `region`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t6` ON `t1`.`region` = `t6`.`region`\n"
            + "GROUP BY `t1`.`@timestamp`, CASE WHEN `t6`.`region` IS NOT NULL THEN `t1`.`region`"
            + " ELSE 'OTHER' END\n"
            + "ORDER BY `t1`.`@timestamp` NULLS LAST, 2 NULLS LAST";
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
        "SELECT `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE 'OTHER'"
            + " END `host`, SUM(`t1`.`$f2`) `avg(cpu_usage)`\n"
            + "FROM (SELECT `SPAN`(`@timestamp`, 1, 'h') `@timestamp`, `host`, AVG(`cpu_usage`)"
            + " `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t1`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2`) `grand_total`\n"
            + "FROM (SELECT `host`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t4`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t6` ON `t1`.`host` = `t6`.`host`\n"
            + "GROUP BY `t1`.`@timestamp`, CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE"
            + " 'OTHER' END\n"
            + "HAVING CASE WHEN `t6`.`host` IS NOT NULL THEN `t1`.`host` ELSE 'OTHER' END <>"
            + " 'OTHER'\n"
            + "ORDER BY `t1`.`@timestamp` NULLS LAST, 2 NULLS LAST";
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
                .add("host", SqlTypeName.VARCHAR)
                .add("region", SqlTypeName.VARCHAR)
                .add("cpu_usage", SqlTypeName.DOUBLE)
                .add("response_time", SqlTypeName.INTEGER)
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
