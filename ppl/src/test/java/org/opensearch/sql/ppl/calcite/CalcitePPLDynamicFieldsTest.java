/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Test suite for PPL queries with dynamic fields in permissive mode. Tests Calcite PPL to Spark SQL
 * conversion for queries that access dynamic/unmapped fields via the _MAP column.
 */
public class CalcitePPLDynamicFieldsTest extends CalcitePPLPermissiveAbstractTest {

  public CalcitePPLDynamicFieldsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testScanTableWithDynamicFields() {
    String ppl = "source=test_dynamic";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testProjectStaticFields() {
    String ppl = "source=test_dynamic | fields id, name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `id`, `name`\n" + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testProjectDynamicField() {
    String ppl = "source=test_dynamic | fields id, status";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], status=[ITEM($2, 'status')])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `_MAP`['status'] `status`\n" + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testProjectMixedFields() {
    String ppl = "source=test_dynamic | fields id, name, status, category";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1], status=[ITEM($2, 'status')], category=[ITEM($2,"
            + " 'category')])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `name`, `_MAP`['status'] `status`, `_MAP`['category'] `category`\n"
            + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterOnDynamicField() {
    String ppl = "source=test_dynamic | where status = 'active'";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[=(ITEM($2, 'status'), 'active':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n" + "FROM `scott`.`test_dynamic`\n" + "WHERE `_MAP`['status'] = 'active'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterOnMixedFields() {
    String ppl = "source=test_dynamic | where id > 10 AND status = 'active'";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[AND(>($0, 10), =(ITEM($2, 'status'), 'active':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "WHERE `id` > 10 AND `_MAP`['status'] = 'active'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSortByDynamicField() {
    String ppl = "source=test_dynamic | sort status";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1], _MAP=[$2])\n"
            + "  LogicalSort(sort0=[$3], dir0=[ASC-nulls-first])\n"
            + "    LogicalProject(id=[$0], name=[$1], _MAP=[$2], $f3=[ITEM($2, 'status')])\n"
            + "      LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `name`, `_MAP`\n"
            + "FROM (SELECT `id`, `name`, `_MAP`, `_MAP`['status'] `$f3`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "ORDER BY 4) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAggregationWithDynamicField() {
    String ppl = "source=test_dynamic | stats count() by status";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$1], status=[$0])\n"
            + "  LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "    LogicalProject(status=[ITEM($2, 'status')])\n"
            + "      LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, `_MAP`['status'] `status`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY `_MAP`['status']";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithDynamicTimestamp() {
    String ppl = "source=test_dynamic | timechart count()";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalProject(@timestamp=[$0], count=[$1])\n"
            + "    LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "      LogicalProject(timestamp=[SPAN(SAFE_CAST(ITEM($2, '@timestamp')), 1, 'm')])\n"
            + "        LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'm') `@timestamp`, COUNT(*)"
            + " `count`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'm')\n"
            + "ORDER BY 1 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTimechartWithDynamicFieldGroupBy() {
    String ppl = "source=test_dynamic | timechart span=1h count() by status";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `@timestamp`, `status`, SUM(`actual_count`) `count`\n"
            + "FROM (SELECT CAST(`t1`.`@timestamp` AS TIMESTAMP) `@timestamp`, CASE WHEN"
            + " `t7`.`status` IS NOT NULL THEN `t1`.`status` ELSE CASE WHEN `t1`.`status` IS NULL"
            + " THEN NULL ELSE 'OTHER' END END `status`, SUM(`t1`.`$f2_0`) `actual_count`\n"
            + "FROM (SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h') `@timestamp`,"
            + " SAFE_CAST(`_MAP`['status'] AS STRING) `status`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY SAFE_CAST(`_MAP`['status'] AS STRING),"
            + " `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')) `t1`\n"
            + "LEFT JOIN (SELECT `status`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h') `@timestamp`,"
            + " SAFE_CAST(`_MAP`['status'] AS STRING) `status`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY SAFE_CAST(`_MAP`['status'] AS STRING),"
            + " `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')) `t4`\n"
            + "WHERE `status` IS NOT NULL\n"
            + "GROUP BY `status`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t7` ON `t1`.`status` IS NOT DISTINCT FROM `t7`.`status`\n"
            + "GROUP BY CAST(`t1`.`@timestamp` AS TIMESTAMP), CASE WHEN `t7`.`status` IS NOT NULL"
            + " THEN `t1`.`status` ELSE CASE WHEN `t1`.`status` IS NULL THEN NULL ELSE 'OTHER' END"
            + " END\n"
            + "UNION\n"
            + "SELECT CAST(`t13`.`@timestamp` AS TIMESTAMP) `@timestamp`, `t24`.`$f0` `status`, 0"
            + " `count`\n"
            + "FROM (SELECT `@timestamp`\n"
            + "FROM (SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')"
            + " `@timestamp`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY SAFE_CAST(`_MAP`['status'] AS STRING),"
            + " `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')) `t12`\n"
            + "GROUP BY `@timestamp`) `t13`\n"
            + "CROSS JOIN (SELECT CASE WHEN `t22`.`status` IS NOT NULL THEN `t16`.`status` ELSE"
            + " CASE WHEN `t16`.`status` IS NULL THEN NULL ELSE 'OTHER' END END `$f0`\n"
            + "FROM (SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h') `@timestamp`,"
            + " SAFE_CAST(`_MAP`['status'] AS STRING) `status`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY SAFE_CAST(`_MAP`['status'] AS STRING),"
            + " `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')) `t16`\n"
            + "LEFT JOIN (SELECT `status`, SUM(`$f2_0`) `grand_total`\n"
            + "FROM (SELECT `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h') `@timestamp`,"
            + " SAFE_CAST(`_MAP`['status'] AS STRING) `status`, COUNT(*) `$f2_0`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "GROUP BY SAFE_CAST(`_MAP`['status'] AS STRING),"
            + " `SPAN`(SAFE_CAST(`_MAP`['@timestamp'] AS STRING), 1, 'h')) `t19`\n"
            + "WHERE `status` IS NOT NULL\n"
            + "GROUP BY `status`\n"
            + "ORDER BY 2 DESC NULLS FIRST\n"
            + "LIMIT 10) `t22` ON `t16`.`status` IS NOT DISTINCT FROM `t22`.`status`\n"
            + "GROUP BY CASE WHEN `t22`.`status` IS NOT NULL THEN `t16`.`status` ELSE CASE WHEN"
            + " `t16`.`status` IS NULL THEN NULL ELSE 'OTHER' END END) `t24`) `t26`\n"
            + "GROUP BY `@timestamp`, `status`\n"
            + "ORDER BY `@timestamp` NULLS LAST, `status` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTrendlineWithDynamicField() {
    String ppl =
        "source=test_dynamic | trendline sma(2, latency) as latency_trend | fields latency,"
            + " latency_trend";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latency=[$3], latency_trend=[CASE(>(COUNT() OVER (ROWS 1 PRECEDING), 1),"
            + " /(SUM(SAFE_CAST($3)) OVER (ROWS 1 PRECEDING), CAST(COUNT($3) OVER (ROWS 1"
            + " PRECEDING)):DOUBLE NOT NULL), null:NULL)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($3)])\n"
            + "    LogicalProject(id=[$0], name=[$1], _MAP=[$2], latency=[ITEM($2, 'latency')])\n"
            + "      LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `latency`, CASE WHEN (COUNT(*) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) >"
            + " 1 THEN (SUM(SAFE_CAST(`latency` AS INTEGER)) OVER (ROWS BETWEEN 1 PRECEDING AND"
            + " CURRENT ROW)) / CAST(COUNT(`latency`) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT"
            + " ROW) AS DOUBLE) ELSE NULL END `latency_trend`\n"
            + "FROM (SELECT `id`, `name`, `_MAP`, `_MAP`['latency'] `latency`\n"
            + "FROM `scott`.`test_dynamic`) `t`\n"
            + "WHERE `latency` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTrendlineSortByDynamicField() {
    String ppl =
        "source=test_dynamic | trendline sort status sma(2, latency) as latency_trend | fields"
            + " latency, latency_trend";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latency=[$4], latency_trend=[CASE(>(COUNT() OVER (ROWS 1 PRECEDING), 1),"
            + " /(SUM(SAFE_CAST($4)) OVER (ROWS 1 PRECEDING), CAST(COUNT($4) OVER (ROWS 1"
            + " PRECEDING)):DOUBLE NOT NULL), null:NULL)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($4)])\n"
            + "    LogicalProject(id=[$0], name=[$1], _MAP=[$2], status=[$3], latency=[ITEM($2,"
            + " 'latency')])\n"
            + "      LogicalSort(sort0=[$3], dir0=[ASC])\n"
            + "        LogicalProject(id=[$0], name=[$1], _MAP=[$2],"
            + " status=[SAFE_CAST(ITEM($2, 'status'))])\n"
            + "          LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `_MAP`['latency'] `latency`, CASE WHEN (COUNT(*) OVER (ROWS BETWEEN 1 PRECEDING"
            + " AND CURRENT ROW)) > 1 THEN (SUM(SAFE_CAST(`_MAP`['latency'] AS INTEGER)) OVER (ROWS"
            + " BETWEEN 1 PRECEDING AND CURRENT ROW)) / CAST(COUNT(`_MAP`['latency']) OVER (ROWS"
            + " BETWEEN 1 PRECEDING AND CURRENT ROW) AS DOUBLE) ELSE NULL END `latency_trend`\n"
            + "FROM (SELECT `id`, `name`, `_MAP`, SAFE_CAST(`_MAP`['status'] AS STRING) `status`\n"
            + "FROM `scott`.`test_dynamic`\n"
            + "ORDER BY 4 NULLS LAST) `t0`\n"
            + "WHERE `_MAP`['latency'] IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsWithDynamicField() {
    String ppl = "source=test_dynamic | eventstats count()";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1], _MAP=[$2], count()=[COUNT() OVER ()])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `name`, `_MAP`, COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `count()`\n"
            + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsByDynamicField() {
    String ppl = "source=test_dynamic | eventstats max(id) by status";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1], _MAP=[$2], max(id)=[MAX($0) OVER (PARTITION BY"
            + " ITEM($2, 'status'))])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `name`, `_MAP`, MAX(`id`) OVER (PARTITION BY `_MAP`['status'] RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `max(id)`\n"
            + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEventstatsAvgDynamicField() {
    String ppl = "source=test_dynamic | eventstats avg(latency) by status";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(id=[$0], name=[$1], _MAP=[$2], avg(latency)=[/(SUM(SAFE_CAST(ITEM($2,"
            + " 'latency'))) OVER (PARTITION BY ITEM($2, 'status')),"
            + " CAST(COUNT(SAFE_CAST(ITEM($2, 'latency'))) OVER (PARTITION BY ITEM($2,"
            + " 'status'))):DOUBLE NOT NULL)])\n"
            + "  LogicalTableScan(table=[[scott, test_dynamic]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `id`, `name`, `_MAP`, (SUM(SAFE_CAST(`_MAP`['latency'] AS INTEGER)) OVER"
            + " (PARTITION BY `_MAP`['status'] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING)) / CAST(COUNT(SAFE_CAST(`_MAP`['latency'] AS INTEGER)) OVER (PARTITION"
            + " BY `_MAP`['status'] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS"
            + " DOUBLE) `avg(latency)`\n"
            + "FROM `scott`.`test_dynamic`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
