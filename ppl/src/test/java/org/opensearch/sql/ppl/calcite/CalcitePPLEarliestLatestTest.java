/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.sql.Date;
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

/** Unit tests for {@code earliest/latest} functions with @timestamp field in PPL. */
public class CalcitePPLEarliestLatestTest extends CalcitePPLAbstractTest {
  public CalcitePPLEarliestLatestTest() {
    super(CalciteAssert.SchemaSpec.POST);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    // Add a test table with @timestamp and created_at fields
    // Note: @timestamp and created_at have different orderings to test explicit field usage
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {
              "server1",
              "ERROR",
              "Database connection failed",
              Date.valueOf("2023-01-01"),
              Date.valueOf("2023-01-05")
            },
            new Object[] {
              "server2",
              "INFO",
              "Service started",
              Date.valueOf("2023-01-02"),
              Date.valueOf("2023-01-04")
            },
            new Object[] {
              "server1",
              "WARN",
              "High memory usage",
              Date.valueOf("2023-01-03"),
              Date.valueOf("2023-01-03")
            },
            new Object[] {
              "server3",
              "ERROR",
              "Disk space low",
              Date.valueOf("2023-01-04"),
              Date.valueOf("2023-01-02")
            },
            new Object[] {
              "server2",
              "INFO",
              "Backup completed",
              Date.valueOf("2023-01-05"),
              Date.valueOf("2023-01-01")
            });
    schema.add("LOGS", new LogsTable(rows));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testEarliestWithoutSecondArgument() {
    String ppl = "source=LOGS | stats earliest(message) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "earliest_message=Database connection failed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`message`, `@timestamp`) `earliest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithoutSecondArgument() {
    String ppl = "source=LOGS | stats latest(message) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(message=[$2], @timestamp=[$3])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "latest_message=Backup completed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`message`, `@timestamp`) `latest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats earliest(message) as earliest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_message=Disk space low; server=server3\n"
            + "earliest_message=Service started; server=server2\n"
            + "earliest_message=Database connection failed; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`message`, `@timestamp`) `earliest_message`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats latest(message) as latest_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latest_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], latest_message=[ARG_MAX($1, $2)])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "latest_message=Disk space low; server=server3\n"
            + "latest_message=Backup completed; server=server2\n"
            + "latest_message=High memory usage; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`message`, `@timestamp`) `latest_message`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithOtherAggregatesWithoutSecondArgument() {
    String ppl =
        "source=LOGS | stats earliest(message) as earliest_message, count() as cnt by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_message=[$1], cnt=[$2], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_message=[ARG_MIN($1, $2)], cnt=[COUNT()])\n"
            + "    LogicalProject(server=[$0], message=[$2], @timestamp=[$3])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_message=Disk space low; cnt=1; server=server3\n"
            + "earliest_message=Service started; cnt=2; server=server2\n"
            + "earliest_message=Database connection failed; cnt=2; server=server1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`message`, `@timestamp`) `earliest_message`, "
            + "COUNT(*) `cnt`, `server`\n"
            + "FROM `POST`.`LOGS`\n"
            + "GROUP BY `server`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithExplicitTimestampField() {
    String ppl = "source=LOGS | stats earliest(message, created_at) as earliest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], earliest_message=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(message=[$2], created_at=[$4])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "earliest_message=Backup completed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`message`, `created_at`) `earliest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithExplicitTimestampField() {
    String ppl = "source=LOGS | stats latest(message, created_at) as latest_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], latest_message=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(message=[$2], created_at=[$4])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "latest_message=Database connection failed\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`message`, `created_at`) `latest_message`\n" + "FROM `POST`.`LOGS`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // Custom table implementation with @timestamp field
  @RequiredArgsConstructor
  public static class LogsTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("server", SqlTypeName.VARCHAR)
                .add("level", SqlTypeName.VARCHAR)
                .add("message", SqlTypeName.VARCHAR)
                .add("@timestamp", SqlTypeName.DATE)
                .add("created_at", SqlTypeName.DATE)
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
