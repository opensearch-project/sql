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

public class CalcitePPLGraphLookupTest extends CalcitePPLAbstractTest {

  public CalcitePPLGraphLookupTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testGraphLookupBasic() {
    // Test basic graphLookup with same source and lookup table
    String ppl =
        "source=employee | graphLookup employee startField=reportsTo fromField=reportsTo"
            + " toField=name as reportingHierarchy";

    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalGraphLookup(fromField=[reportsTo], toField=[name],"
            + " outputField=[reportingHierarchy], depthField=[null], maxDepth=[-1],"
            + " bidirectional=[false])\n"
            + "  LogicalSort(fetch=[100])\n"
            + "    LogicalTableScan(table=[[scott, employee]])\n"
            + "  LogicalTableScan(table=[[scott, employee]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testGraphLookupWithDepthField() {
    // Test graphLookup with depthField parameter
    String ppl =
        "source=employee | graphLookup employee startField=reportsTo fromField=reportsTo"
            + " toField=name depthField=level as reportingHierarchy";

    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalGraphLookup(fromField=[reportsTo], toField=[name],"
            + " outputField=[reportingHierarchy], depthField=[Field(field=level, fieldArgs=[])],"
            + " maxDepth=[-1], bidirectional=[false])\n"
            + "  LogicalSort(fetch=[100])\n"
            + "    LogicalTableScan(table=[[scott, employee]])\n"
            + "  LogicalTableScan(table=[[scott, employee]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testGraphLookupWithMaxDepth() {
    // Test graphLookup with maxDepth parameter
    String ppl =
        "source=employee | graphLookup employee startField=reportsTo fromField=reportsTo"
            + " toField=name maxDepth=3 as reportingHierarchy";

    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalGraphLookup(fromField=[reportsTo], toField=[name],"
            + " outputField=[reportingHierarchy], depthField=[null], maxDepth=[3],"
            + " bidirectional=[false])\n"
            + "  LogicalSort(fetch=[100])\n"
            + "    LogicalTableScan(table=[[scott, employee]])\n"
            + "  LogicalTableScan(table=[[scott, employee]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testGraphLookupBidirectional() {
    // Test graphLookup with bidirectional traversal
    String ppl =
        "source=employee | graphLookup employee startField=reportsTo fromField=reportsTo"
            + " toField=name direction=bi as reportingHierarchy";

    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalGraphLookup(fromField=[reportsTo], toField=[name],"
            + " outputField=[reportingHierarchy], depthField=[null], maxDepth=[-1],"
            + " bidirectional=[true])\n"
            + "  LogicalSort(fetch=[100])\n"
            + "    LogicalTableScan(table=[[scott, employee]])\n"
            + "  LogicalTableScan(table=[[scott, employee]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    // Add employee table for graphLookup tests
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {1, "Dev", null},
            new Object[] {2, "Eliot", "Dev"},
            new Object[] {3, "Ron", "Eliot"},
            new Object[] {4, "Andrew", "Eliot"},
            new Object[] {5, "Asya", "Ron"},
            new Object[] {6, "Dan", "Andrew"});
    schema.add("employee", new EmployeeTable(rows));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @RequiredArgsConstructor
  public static class EmployeeTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("id", SqlTypeName.INTEGER)
                .nullable(false)
                .add("name", SqlTypeName.VARCHAR)
                .nullable(false)
                .add("reportsTo", SqlTypeName.VARCHAR)
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
