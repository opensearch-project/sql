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

/**
 * Unit test for GitHub issue #5174: bin/chart NPE with null values.
 *
 * <p>Verifies that the chart command generates correct logical plans when the input contains null
 * values from binning, and that the sort operations properly handle nulls.
 */
public class CalcitePPLChartNullTest extends CalcitePPLAbstractTest {

  public CalcitePPLChartNullTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Table with null values matching the issue's bounty-numbers schema
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {1, "A", "X", 10.5},
            new Object[] {2, "A", "Y", 20.3},
            new Object[] {10, "B", "X", 100.0},
            new Object[] {null, "B", "Y", null});
    schema.add("bounty_numbers", new BountyNumbersTable(rows));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testBinThenChartWithNullValuesLogicalPlan() {
    String ppl =
        "source=bounty_numbers | bin value span=50 as val_bin"
            + " | chart count() over val_bin by category";
    RelNode root = getRelNode(ppl);
    // Verify the SQL plan contains WHERE val_bin IS NOT NULL to filter null bin values,
    // and NULLS LAST in ORDER BY for proper null handling in sort
    String expectedSparkSql =
        "SELECT `t2`.`val_bin`, CASE WHEN `t2`.`category` IS NULL THEN 'NULL' WHEN"
            + " `t10`.`_row_number_chart_` <= 10 THEN `t2`.`category` ELSE 'OTHER' END"
            + " `category`, SUM(`t2`.`count()`) `count()`\n"
            + "FROM (SELECT `val_bin`, `category`, COUNT(*) `count()`\n"
            + "FROM (SELECT `count`, `category`, `subcategory`, `value`,"
            + " SPAN_BUCKET(`value`, 50) `val_bin`\n"
            + "FROM `scott`.`bounty_numbers`) `t`\n"
            + "WHERE `val_bin` IS NOT NULL\n"
            + "GROUP BY `val_bin`, `category`) `t2`\n"
            + "LEFT JOIN (SELECT `category`, SUM(`count()`) `__grand_total__`, ROW_NUMBER() OVER"
            + " (ORDER BY SUM(`count()`) DESC) `_row_number_chart_`\n"
            + "FROM (SELECT `category`, COUNT(*) `count()`\n"
            + "FROM (SELECT `count`, `category`, `subcategory`, `value`,"
            + " SPAN_BUCKET(`value`, 50) `val_bin`\n"
            + "FROM `scott`.`bounty_numbers`) `t3`\n"
            + "WHERE `val_bin` IS NOT NULL\n"
            + "GROUP BY `val_bin`, `category`) `t7`\n"
            + "WHERE `category` IS NOT NULL\n"
            + "GROUP BY `category`) `t10` ON `t2`.`category` = `t10`.`category`\n"
            + "GROUP BY `t2`.`val_bin`, CASE WHEN `t2`.`category` IS NULL THEN 'NULL' WHEN"
            + " `t10`.`_row_number_chart_` <= 10 THEN `t2`.`category` ELSE 'OTHER' END\n"
            + "ORDER BY `t2`.`val_bin` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBinThenChartSingleGroupWithNullValuesLogicalPlan() {
    String ppl =
        "source=bounty_numbers | bin value span=50 as val_bin | chart count() over val_bin";
    RelNode root = getRelNode(ppl);
    // Verify null bin values are filtered and sort uses NULLS LAST
    String expectedSparkSql =
        "SELECT `val_bin`, COUNT(*) `count()`\n"
            + "FROM (SELECT `count`, `category`, `subcategory`, `value`,"
            + " SPAN_BUCKET(`value`, 50) `val_bin`\n"
            + "FROM `scott`.`bounty_numbers`) `t`\n"
            + "WHERE `val_bin` IS NOT NULL\n"
            + "GROUP BY `val_bin`\n"
            + "ORDER BY `val_bin` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @RequiredArgsConstructor
  public static class BountyNumbersTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("count", SqlTypeName.INTEGER)
                .nullable(true)
                .add("category", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("subcategory", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("value", SqlTypeName.DOUBLE)
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
