/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class CalcitePPLChartTest extends CalcitePPLAbstractTest {

  public CalcitePPLChartTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add events table for chart tests - similar to bank data used in integration tests
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {32838, "F", 28, "VA", java.sql.Timestamp.valueOf("2024-07-01 00:00:00")},
            new Object[] {40540, "F", 39, "PA", java.sql.Timestamp.valueOf("2024-07-01 00:01:00")},
            new Object[] {39225, "M", 32, "IL", java.sql.Timestamp.valueOf("2024-07-01 00:02:00")},
            new Object[] {4180, "M", 33, "MD", java.sql.Timestamp.valueOf("2024-07-01 00:03:00")},
            new Object[] {11052, "M", 36, "WA", java.sql.Timestamp.valueOf("2024-07-01 00:04:00")},
            new Object[] {48086, "F", 34, "IN", java.sql.Timestamp.valueOf("2024-07-01 00:05:00")});
    schema.add("bank", new BankTable(rows));

    // Add time_data table for span tests
    ImmutableList<Object[]> timeRows =
        ImmutableList.of(
            new Object[] {java.sql.Timestamp.valueOf("2025-07-28 00:00:00"), "A", 9367},
            new Object[] {java.sql.Timestamp.valueOf("2025-07-29 00:00:00"), "B", 9521},
            new Object[] {java.sql.Timestamp.valueOf("2025-07-30 00:00:00"), "C", 9187},
            new Object[] {java.sql.Timestamp.valueOf("2025-07-31 00:00:00"), "D", 8736},
            new Object[] {java.sql.Timestamp.valueOf("2025-08-01 00:00:00"), "A", 9015});
    schema.add("time_data", new TimeDataTable(timeRows));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testChartWithSingleGroupKey() {
    String ppl = "source=bank | chart avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithOverSyntax() {
    String ppl = "source=bank | chart avg(balance) over gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithMultipleGroupKeys() {
    String ppl = "source=bank | chart avg(balance) over gender by age";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`gender`, CASE WHEN `t1`.`age` IS NULL THEN 'NULL' WHEN `t6`.`__row_number__`"
            + " <= 10 THEN `t1`.`age` ELSE 'OTHER' END `age`, AVG(`t1`.`avg(balance)`)"
            + " `avg(balance)`\n"
            + "FROM (SELECT AVG(`balance`) `avg(balance)`, `gender`, SAFE_CAST(`age` AS STRING)"
            + " `age`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`, `age`) `t1`\n"
            + "LEFT JOIN (SELECT `age`, AVG(`avg(balance)`) `__grand_total__`, ROW_NUMBER() OVER"
            + " (ORDER BY AVG(`avg(balance)`) DESC) `__row_number__`\n"
            + "FROM (SELECT AVG(`balance`) `avg(balance)`, SAFE_CAST(`age` AS STRING) `age`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`, `age`) `t4`\n"
            + "GROUP BY `age`) `t6` ON `t1`.`age` = `t6`.`age`\n"
            + "GROUP BY `t1`.`gender`, CASE WHEN `t1`.`age` IS NULL THEN 'NULL' WHEN"
            + " `t6`.`__row_number__` <= 10 THEN `t1`.`age` ELSE 'OTHER' END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithMultipleGroupKeysAlternativeSyntax() {
    String ppl = "source=bank | chart avg(balance) by gender, age";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`gender`, CASE WHEN `t1`.`age` IS NULL THEN 'NULL' WHEN `t6`.`__row_number__`"
            + " <= 10 THEN `t1`.`age` ELSE 'OTHER' END `age`, AVG(`t1`.`avg(balance)`)"
            + " `avg(balance)`\n"
            + "FROM (SELECT AVG(`balance`) `avg(balance)`, `gender`, SAFE_CAST(`age` AS STRING)"
            + " `age`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`, `age`) `t1`\n"
            + "LEFT JOIN (SELECT `age`, AVG(`avg(balance)`) `__grand_total__`, ROW_NUMBER() OVER"
            + " (ORDER BY AVG(`avg(balance)`) DESC) `__row_number__`\n"
            + "FROM (SELECT AVG(`balance`) `avg(balance)`, SAFE_CAST(`age` AS STRING) `age`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`, `age`) `t4`\n"
            + "GROUP BY `age`) `t6` ON `t1`.`age` = `t6`.`age`\n"
            + "GROUP BY `t1`.`gender`, CASE WHEN `t1`.`age` IS NULL THEN 'NULL' WHEN"
            + " `t6`.`__row_number__` <= 10 THEN `t1`.`age` ELSE 'OTHER' END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithLimit() {
    String ppl = "source=bank | chart limit=2 avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithLimitZero() {
    String ppl = "source=bank | chart limit=0 avg(balance) over state by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `state`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `state`, `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithSpan() {
    String ppl = "source=bank | chart max(balance) by age span=10";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT MAX(`balance`) `max(balance)`, `SPAN`(`age`, 10, NULL) `age`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `SPAN`(`age`, 10, NULL)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithTimeSpan() {
    String ppl = "source=time_data | chart max(value) over timestamp span=1week by category";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `t1`.`timestamp`, CASE WHEN `t1`.`category` IS NULL THEN 'NULL' WHEN"
            + " `t6`.`__row_number__` <= 10 THEN `t1`.`category` ELSE 'OTHER' END `category`,"
            + " MAX(`t1`.`max(value)`) `max(value)`\n"
            + "FROM (SELECT MAX(`value`) `max(value)`, `SPAN`(`timestamp`, 1, 'w') `timestamp`,"
            + " `category`\n"
            + "FROM `scott`.`time_data`\n"
            + "GROUP BY `category`, `SPAN`(`timestamp`, 1, 'w')) `t1`\n"
            + "LEFT JOIN (SELECT `category`, MAX(`max(value)`) `__grand_total__`, ROW_NUMBER() OVER"
            + " (ORDER BY MAX(`max(value)`) DESC) `__row_number__`\n"
            + "FROM (SELECT MAX(`value`) `max(value)`, `category`\n"
            + "FROM `scott`.`time_data`\n"
            + "GROUP BY `category`, `SPAN`(`timestamp`, 1, 'w')) `t4`\n"
            + "GROUP BY `category`) `t6` ON `t1`.`category` = `t6`.`category`\n"
            + "GROUP BY `t1`.`timestamp`, CASE WHEN `t1`.`category` IS NULL THEN 'NULL' WHEN"
            + " `t6`.`__row_number__` <= 10 THEN `t1`.`category` ELSE 'OTHER' END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithUseOtherTrue() {
    String ppl = "source=bank | chart useother=true avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithUseOtherFalse() {
    String ppl = "source=bank | chart useother=false limit=2 avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithOtherStr() {
    String ppl = "source=bank | chart limit=1 otherstr='other_values' avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithNullStr() {
    String ppl = "source=bank | chart nullstr='null_values' avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChartWithUseNull() {
    String ppl = "source=bank | chart usenull=false avg(balance) by gender";

    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT AVG(`balance`) `avg(balance)`, `gender`\n"
            + "FROM `scott`.`bank`\n"
            + "WHERE `gender` IS NOT NULL\n"
            + "GROUP BY `gender`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  private UnresolvedPlan parsePPL(String query) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    AstBuilder astBuilder = new AstBuilder(query);
    return astBuilder.visit(parser.parse(query));
  }

  @RequiredArgsConstructor
  public static class BankTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("balance", SqlTypeName.INTEGER)
                .nullable(true)
                .add("gender", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("age", SqlTypeName.INTEGER)
                .nullable(true)
                .add("state", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("timestamp", SqlTypeName.TIMESTAMP)
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

  @RequiredArgsConstructor
  public static class TimeDataTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("timestamp", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("category", SqlTypeName.VARCHAR)
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
