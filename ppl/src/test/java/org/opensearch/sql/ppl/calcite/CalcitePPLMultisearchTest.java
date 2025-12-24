/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;
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

public class CalcitePPLMultisearchTest extends CalcitePPLAbstractTest {

  public CalcitePPLMultisearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    // Add timestamp tables for multisearch testing with format matching time_test_data.json
    ImmutableList<Object[]> timeData1 =
        ImmutableList.of(
            new Object[] {
              Timestamp.valueOf("2025-08-01 03:47:41"),
              8762,
              "A",
              Timestamp.valueOf("2025-08-01 03:47:41")
            },
            new Object[] {
              Timestamp.valueOf("2025-08-01 01:14:11"),
              9015,
              "B",
              Timestamp.valueOf("2025-08-01 01:14:11")
            },
            new Object[] {
              Timestamp.valueOf("2025-07-31 23:40:33"),
              8676,
              "A",
              Timestamp.valueOf("2025-07-31 23:40:33")
            },
            new Object[] {
              Timestamp.valueOf("2025-07-31 21:07:03"),
              8490,
              "B",
              Timestamp.valueOf("2025-07-31 21:07:03")
            });

    ImmutableList<Object[]> timeData2 =
        ImmutableList.of(
            new Object[] {
              Timestamp.valueOf("2025-08-01 04:00:00"),
              2001,
              "E",
              Timestamp.valueOf("2025-08-01 04:00:00")
            },
            new Object[] {
              Timestamp.valueOf("2025-08-01 02:30:00"),
              2002,
              "F",
              Timestamp.valueOf("2025-08-01 02:30:00")
            },
            new Object[] {
              Timestamp.valueOf("2025-08-01 01:00:00"),
              2003,
              "E",
              Timestamp.valueOf("2025-08-01 01:00:00")
            },
            new Object[] {
              Timestamp.valueOf("2025-07-31 22:15:00"),
              2004,
              "F",
              Timestamp.valueOf("2025-07-31 22:15:00")
            });

    schema.add("TIME_DATA1", new TimeDataTable(timeData1));
    schema.add("TIME_DATA2", new TimeDataTable(timeData2));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testBasicMultisearch() {
    String ppl =
        "| multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 20]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    verifyResultCount(root, 8);
  }

  @Test
  public void testMultisearchCrossIndices() {
    // Test multisearch with different tables (indices)
    String ppl =
        "| multisearch [search source=EMP | where DEPTNO = 10 | fields EMPNO, ENAME,"
            + " JOB] [search source=DEPT | where DEPTNO = 10 | fields DEPTNO, DNAME, LOC]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], DEPTNO=[null:TINYINT],"
            + " DNAME=[null:VARCHAR(14)], LOC=[null:VARCHAR(13)])\n"
            + "    LogicalFilter(condition=[=($7, 10)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
            + "    LogicalFilter(condition=[=($0, 10)])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, CAST(NULL AS TINYINT) `DEPTNO`, CAST(NULL AS STRING)"
            + " `DNAME`, CAST(NULL AS STRING) `LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS SMALLINT) `EMPNO`, CAST(NULL AS STRING) `ENAME`, CAST(NULL AS"
            + " STRING) `JOB`, `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` = 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    verifyResultCount(root, 4); // 3 employees + 1 department
  }

  @Test
  public void testMultisearchWithStats() {
    String ppl =
        "| multisearch "
            + "[search source=EMP | where DEPTNO = 10 | eval type = \"accounting\"] "
            + "[search source=EMP | where DEPTNO = 20 | eval type = \"research\"] "
            + "| stats count by type";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count=[$1], type=[$0])\n"
            + "  LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "    LogicalProject(type=[$8])\n"
            + "      LogicalUnion(all=[true])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], type=['accounting':VARCHAR])\n"
            + "          LogicalFilter(condition=[=($7, 10)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], type=['research':VARCHAR])\n"
            + "          LogicalFilter(condition=[=($7, 20)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count`, `type`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " 'accounting' `type`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "UNION ALL\n"
            + "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " 'research' `type`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20) `t3`\n"
            + "GROUP BY `type`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    verifyResultCount(root, 2);
  }

  @Test
  public void testMultisearchThreeSubsearches() {
    String ppl =
        "| multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 20] "
            + "[search source=EMP | where DEPTNO = 30]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 30)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 30";
    verifyPPLToSparkSQL(root, expectedSparkSql);
    verifyResultCount(root, 14);
  }

  // ========================================================================
  // Timestamp Interleaving Tests
  // ========================================================================

  @Test
  public void testMultisearchTimestampInterleaving() {
    String ppl =
        "| multisearch "
            + "[search source=TIME_DATA1 | where category IN (\"A\", \"B\")] "
            + "[search source=TIME_DATA2 | where category IN (\"E\", \"F\")] "
            + "| head 6";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$3], dir0=[DESC], fetch=[6])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalFilter(condition=[SEARCH($2, Sarg['A':VARCHAR, 'B':VARCHAR]:VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, TIME_DATA1]])\n"
            + "    LogicalFilter(condition=[SEARCH($2, Sarg['E':VARCHAR, 'F':VARCHAR]:VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, TIME_DATA2]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`TIME_DATA1`\n"
            + "WHERE `category` IN ('A', 'B')\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`TIME_DATA2`\n"
            + "WHERE `category` IN ('E', 'F'))\n"
            + "ORDER BY `@timestamp` DESC NULLS FIRST\n"
            + "LIMIT 6";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultisearchWithTimestampFiltering() {
    String ppl =
        "| multisearch "
            + "[search source=TIME_DATA1 | where @timestamp > \"2025-07-31 23:00:00\"] "
            + "[search source=TIME_DATA2 | where @timestamp > \"2025-07-31 23:00:00\"] "
            + "| sort @timestamp desc";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$3], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(sort0=[$3], dir0=[DESC])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalFilter(condition=[>($3, '2025-07-31 23:00:00':VARCHAR)])\n"
            + "        LogicalTableScan(table=[[scott, TIME_DATA1]])\n"
            + "      LogicalFilter(condition=[>($3, '2025-07-31 23:00:00':VARCHAR)])\n"
            + "        LogicalTableScan(table=[[scott, TIME_DATA2]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `timestamp`, `value`, `category`, `@timestamp`\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`TIME_DATA1`\n"
            + "WHERE `@timestamp` > '2025-07-31 23:00:00'\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`TIME_DATA2`\n"
            + "WHERE `@timestamp` > '2025-07-31 23:00:00')\n"
            + "ORDER BY `@timestamp` DESC NULLS FIRST) `t2`\n"
            + "ORDER BY `@timestamp` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // ========================================================================
  // Custom Table Implementation for Timestamp Testing
  // ========================================================================

  /** Custom table implementation with timestamp fields for multisearch testing. */
  @RequiredArgsConstructor
  static class TimeDataTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("timestamp", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("value", SqlTypeName.INTEGER)
                .nullable(true)
                .add("category", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("@timestamp", SqlTypeName.TIMESTAMP)
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
