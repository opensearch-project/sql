/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class CalcitePPLTimechartTest extends CalcitePPLAbstractTest {

  public CalcitePPLTimechartTest() {
    super(CalciteAssert.SchemaSpec.JDBC_FOODMART);
  }

  @Test
  public void testTimechartBasicSyntax() {
    String ppl = "source=events | timechart count()";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithSpanSyntax() {
    String ppl = "source=events | timechart span(@timestamp, 1m) count()";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithBySyntax() {
    String ppl = "source=events | timechart count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithLimitSyntax() {
    String ppl = "source=events | timechart limit=5 count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithSpan1h() {
    String ppl = "source=events | timechart span=1h count() by host";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithSpan1m() {
    String ppl = "source=events | timechart span=1m avg(cpu_usage) by region";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
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
  public void testTimechartToSparkSQL() {
    String ppl = "source=foodmart.sales_fact_1997 | eval `@timestamp` = CAST(time_id AS STRING) | timechart span=1h count() by product_id";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql = 
        "SELECT `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END `product_id0`, SUM(`t0`.`$f2_0`) `count`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t0`\n" +
        "LEFT JOIN (SELECT `$f2`, SUM(`$f2_0`) `grand_total`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t2`\n" +
        "GROUP BY `$f2`\n" +
        "ORDER BY 2 DESC NULLS FIRST\n" +
        "LIMIT 10) `t4` ON `t0`.`$f2` = `t4`.`$f2`\n" +
        "GROUP BY `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END\n" +
        "ORDER BY `t0`.`product_id` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimitToSparkSQL() {
    String ppl = "source=foodmart.sales_fact_1997 | eval `@timestamp` = CAST(time_id AS STRING) | timechart span=1h limit=5 count() by product_id";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql = 
        "SELECT `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END `product_id0`, SUM(`t0`.`$f2_0`) `count`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t0`\n" +
        "LEFT JOIN (SELECT `$f2`, SUM(`$f2_0`) `grand_total`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t2`\n" +
        "GROUP BY `$f2`\n" +
        "ORDER BY 2 DESC NULLS FIRST\n" +
        "LIMIT 5) `t4` ON `t0`.`$f2` = `t4`.`$f2`\n" +
        "GROUP BY `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END\n" +
        "ORDER BY `t0`.`product_id` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithUseOtherToSparkSQL() {
    String ppl = "source=foodmart.sales_fact_1997 | eval `@timestamp` = CAST(time_id AS STRING) | timechart span=1h useother=true count() by product_id";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql = 
        "SELECT `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END `product_id0`, SUM(`t0`.`$f2_0`) `count`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t0`\n" +
        "LEFT JOIN (SELECT `$f2`, SUM(`$f2_0`) `grand_total`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t2`\n" +
        "GROUP BY `$f2`\n" +
        "ORDER BY 2 DESC NULLS FIRST\n" +
        "LIMIT 10) `t4` ON `t0`.`$f2` = `t4`.`$f2`\n" +
        "GROUP BY `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END\n" +
        "ORDER BY `t0`.`product_id` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithLimitAndUseOtherToSparkSQL() {
    String ppl = "source=foodmart.sales_fact_1997 | eval `@timestamp` = CAST(time_id AS STRING) | timechart span=1h limit=5 useother=true count() by product_id";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql = 
        "SELECT `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END `product_id0`, SUM(`t0`.`$f2_0`) `count`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t0`\n" +
        "LEFT JOIN (SELECT `$f2`, SUM(`$f2_0`) `grand_total`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f2`, COUNT(*) `$f2_0`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t2`\n" +
        "GROUP BY `$f2`\n" +
        "ORDER BY 2 DESC NULLS FIRST\n" +
        "LIMIT 5) `t4` ON `t0`.`$f2` = `t4`.`$f2`\n" +
        "GROUP BY `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f2` ELSE 'OTHER' END\n" +
        "ORDER BY `t0`.`product_id` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithAvgToSparkSQL() {
    String ppl = "source=foodmart.sales_fact_1997 | eval `@timestamp` = CAST(time_id AS STRING) | timechart span=1h avg(store_sales) by product_id";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql = 
        "SELECT `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f3` ELSE 'OTHER' END `product_id0`, SUM(`t0`.`$f2`) `avg`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f3`, AVG(`store_sales`) `$f2`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t0`\n" +
        "LEFT JOIN (SELECT `$f3`, SUM(`$f2`) `grand_total`\n" +
        "FROM (SELECT `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h') `$f3`, AVG(`store_sales`) `$f2`\n" +
        "FROM `foodmart`.`sales_fact_1997`\n" +
        "GROUP BY `product_id`, `SPAN`(SAFE_CAST(`time_id` AS STRING), 1, 'h')) `t2`\n" +
        "GROUP BY `$f3`\n" +
        "ORDER BY 2 DESC NULLS FIRST\n" +
        "LIMIT 10) `t4` ON `t0`.`$f3` = `t4`.`$f3`\n" +
        "GROUP BY `t0`.`product_id`, CASE WHEN `t4`.`grand_total` IS NOT NULL THEN `t0`.`$f3` ELSE 'OTHER' END\n" +
        "ORDER BY `t0`.`product_id` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  private UnresolvedPlan parsePPL(String query) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    AstBuilder astBuilder = new AstBuilder(query);
    return astBuilder.visit(parser.parse(query));
  }
}
