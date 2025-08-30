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
        "SELECT `t1`.`$f2` `@timestamp`, `t6`.`host`, COALESCE(`t14`.`actual_count`, 0) `count`\n"
            + "FROM (SELECT `$f2`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t0`\n"
            + "GROUP BY `$f2`) `t1`\n"
            + "CROSS JOIN (SELECT `host`\n"
            + "FROM (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t3`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t5`) `t6`\n"
            + "LEFT JOIN (SELECT `t8`.`$f2`, CASE WHEN `t12`.`host` IS NOT NULL THEN `t8`.`host`"
            + " ELSE 'OTHER' END `$f1`, SUM(`t8`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t8`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t10`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t12` ON `t8`.`host` = `t12`.`host`\n"
            + "GROUP BY `t8`.`$f2`, CASE WHEN `t12`.`host` IS NOT NULL THEN `t8`.`host` ELSE"
            + " 'OTHER' END) `t14` ON `t1`.`$f2` = `t14`.`$f2` AND `t6`.`host` = `t14`.`$f1`\n"
            + "ORDER BY `t1`.`$f2` NULLS LAST, `t6`.`host` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimit() {
    String ppl = "source=events | timechart limit=3 count() by host";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`$f2` `@timestamp`, `t12`.`other_category` `host`,"
            + " COALESCE(`t20`.`actual_count`, 0) `count`\n"
            + "FROM (SELECT `$f2`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t0`\n"
            + "GROUP BY `$f2`) `t1`\n"
            + "CROSS JOIN (SELECT 'OTHER' `other_category`\n"
            + "FROM (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t3`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t5`\n"
            + "UNION\n"
            + "SELECT `host`\n"
            + "FROM (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t8`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t10`) `t12`\n"
            + "LEFT JOIN (SELECT `t14`.`$f2`, CASE WHEN `t18`.`host` IS NOT NULL THEN `t14`.`host`"
            + " ELSE 'OTHER' END `$f1`, SUM(`t14`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t14`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'm') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'm')) `t16`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t18` ON `t14`.`host` = `t18`.`host`\n"
            + "GROUP BY `t14`.`$f2`, CASE WHEN `t18`.`host` IS NOT NULL THEN `t14`.`host` ELSE"
            + " 'OTHER' END) `t20` ON `t1`.`$f2` = `t20`.`$f2` AND `t12`.`other_category` ="
            + " `t20`.`$f1`\n"
            + "ORDER BY `t1`.`$f2` NULLS LAST, `t12`.`other_category` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1h() {
    String ppl = "source=events | timechart span=1h count() by host";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`$f2` `@timestamp`, `t6`.`host`, COALESCE(`t14`.`actual_count`, 0) `count`\n"
            + "FROM (SELECT `$f2`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t0`\n"
            + "GROUP BY `$f2`) `t1`\n"
            + "CROSS JOIN (SELECT `host`\n"
            + "FROM (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t3`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t5`) `t6`\n"
            + "LEFT JOIN (SELECT `t8`.`$f2`, CASE WHEN `t12`.`host` IS NOT NULL THEN `t8`.`host`"
            + " ELSE 'OTHER' END `$f1`, SUM(`t8`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t8`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f2`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t10`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t12` ON `t8`.`host` = `t12`.`host`\n"
            + "GROUP BY `t8`.`$f2`, CASE WHEN `t12`.`host` IS NOT NULL THEN `t8`.`host` ELSE"
            + " 'OTHER' END) `t14` ON `t1`.`$f2` = `t14`.`$f2` AND `t6`.`host` = `t14`.`$f1`\n"
            + "ORDER BY `t1`.`$f2` NULLS LAST, `t6`.`host` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithSpan1m() {
    String ppl = "source=events | timechart span=1m avg(cpu_usage) by region";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t0`.`$f3` `@timestamp`, CASE WHEN `t4`.`region` IS NOT NULL THEN `t0`.`region`"
            + " ELSE 'OTHER' END `region`, SUM(`t0`.`$f2`) `avg(cpu_usage)`\n"
            + "FROM (SELECT `region`, `SPAN`(`@timestamp`, 1, 'm') `$f3`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `region`, `SPAN`(`@timestamp`, 1, 'm')) `t0`\n"
            + "LEFT JOIN (SELECT `region`, SUM(`$f2`) `grand_total`\n"
            + "FROM (SELECT `region`, `SPAN`(`@timestamp`, 1, 'm') `$f3`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `region`, `SPAN`(`@timestamp`, 1, 'm')) `t2`\n"
            + "GROUP BY `region`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t4` ON `t0`.`region` = `t4`.`region`\n"
            + "GROUP BY `t0`.`$f3`, CASE WHEN `t4`.`region` IS NOT NULL THEN `t0`.`region` ELSE"
            + " 'OTHER' END\n"
            + "ORDER BY `t0`.`$f3` NULLS LAST, 2 NULLS LAST";
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
        "SELECT `t0`.`$f3` `@timestamp`, CASE WHEN `t4`.`host` IS NOT NULL THEN `t0`.`host` ELSE"
            + " 'OTHER' END `host`, SUM(`t0`.`$f2`) `avg(cpu_usage)`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f3`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t0`\n"
            + "LEFT JOIN (SELECT `host`, SUM(`$f2`) `grand_total`\n"
            + "FROM (SELECT `host`, `SPAN`(`@timestamp`, 1, 'h') `$f3`, AVG(`cpu_usage`) `$f2`\n"
            + "FROM `scott`.`events`\n"
            + "GROUP BY `host`, `SPAN`(`@timestamp`, 1, 'h')) `t2`\n"
            + "GROUP BY `host`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 3) `t4` ON `t0`.`host` = `t4`.`host`\n"
            + "GROUP BY `t0`.`$f3`, CASE WHEN `t4`.`host` IS NOT NULL THEN `t0`.`host` ELSE 'OTHER'"
            + " END\n"
            + "HAVING CASE WHEN `t4`.`host` IS NOT NULL THEN `t0`.`host` ELSE 'OTHER' END <>"
            + " 'OTHER'\n"
            + "ORDER BY `t0`.`$f3` NULLS LAST, 2 NULLS LAST";
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
