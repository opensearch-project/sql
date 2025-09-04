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

/** Unit tests for {@code first/last} functions in document order in PPL. */
public class CalcitePPLFirstLastTest extends CalcitePPLAbstractTest {
  public CalcitePPLFirstLastTest() {
    super(CalciteAssert.SchemaSpec.POST);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    // Add a test table with data for testing first/last in document order
    // Note: The order of rows in the list represents the document order
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
  public void testFirstWithoutSecondArgument() {
    String ppl = "source=LOGS | stats first(message) as first_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], first_message=[FIRST($0)])\n"
            + "  LogicalProject(message=[$2])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "first_message=Database connection failed\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testLastWithoutSecondArgument() {
    String ppl = "source=LOGS | stats last(message) as last_message";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], last_message=[LAST($0)])\n"
            + "  LogicalProject(message=[$2])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "last_message=Backup completed\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testFirstByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats first(message) as first_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(first_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], first_message=[FIRST($1)])\n"
            + "    LogicalProject(server=[$0], message=[$2])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "first_message=Disk space low; server=server3\n"
            + "first_message=Service started; server=server2\n"
            + "first_message=Database connection failed; server=server1\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testLastByServerWithoutSecondArgument() {
    String ppl = "source=LOGS | stats last(message) as last_message by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(last_message=[$1], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], last_message=[LAST($1)])\n"
            + "    LogicalProject(server=[$0], message=[$2])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "last_message=Disk space low; server=server3\n"
            + "last_message=Backup completed; server=server2\n"
            + "last_message=High memory usage; server=server1\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testFirstWithOtherAggregatesWithoutSecondArgument() {
    String ppl = "source=LOGS | stats first(message) as first_message, count() as cnt by server";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(first_message=[$1], cnt=[$2], server=[$0])\n"
            + "  LogicalAggregate(group=[{0}], first_message=[FIRST($1)], cnt=[COUNT()])\n"
            + "    LogicalProject(server=[$0], message=[$2])\n"
            + "      LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "first_message=Disk space low; cnt=1; server=server3\n"
            + "first_message=Service started; cnt=2; server=server2\n"
            + "first_message=Database connection failed; cnt=2; server=server1\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testFirstLastDifferentFields() {
    String ppl = "source=LOGS | stats first(server) as first_server, last(level) as last_level";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], first_server=[FIRST($0)], last_level=[LAST($1)])\n"
            + "  LogicalProject(server=[$0], level=[$1])\n"
            + "    LogicalTableScan(table=[[POST, LOGS]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "first_server=server1; last_level=INFO\n";
    verifyResult(root, expectedResult);
  }

  // Custom table implementation
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
