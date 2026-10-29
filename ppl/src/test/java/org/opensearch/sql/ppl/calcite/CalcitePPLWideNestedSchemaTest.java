/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
 * The implicit {@code fields *} projection drops flattened sub-fields whose parent struct is also a
 * column. Index patterns over many indices with dynamic sub-fields (e.g. {@code attributes.*}) can
 * produce 100k+ columns, so this pass must be linear in the column count.
 */
public class CalcitePPLWideNestedSchemaTest extends CalcitePPLAbstractTest {

  private static final int WIDE_SUBFIELDS = 100_000;

  public CalcitePPLWideNestedSchemaTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("SMALL", new WideNestedTable(3));
    schema.add("WIDE", new WideNestedTable(WIDE_SUBFIELDS));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * Sub-fields with an existing parent column are dropped (including multi-level ones), a dotted
   * field without a parent column is kept, and the remaining columns keep their original order.
   */
  @Test
  public void testNestedSubFieldsRemovedAndOrderPreserved() {
    RelNode root = getRelNode("source=SMALL");
    String expectedLogical =
        "LogicalProject(ID=[$0], ATTRS=[$1], LONE.X=[$3], NAME=[$8])\n"
            + "  LogicalTableScan(table=[[scott, SMALL]])\n";
    verifyLogical(root, expectedLogical);
  }

  /** Quadratic name lookups made this take ~36s at ~112k columns (V2429433093). */
  @Test(timeout = 10_000)
  public void testWideNestedSchemaIsLinear() {
    RelNode root = getRelNode("source=WIDE");
    String expectedLogical =
        "LogicalProject(ID=[$0], ATTRS=[$1], LONE.X=[$3], NAME=[$"
            + (WIDE_SUBFIELDS + 5)
            + "])\n"
            + "  LogicalTableScan(table=[[scott, WIDE]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * Columns: ID, ATTRS (struct), ATTRS.SUB (struct), LONE.X (no LONE parent), ATTRS.SUB.X, then
   * {@code subfields} flattened ATTRS.F* leaves, then NAME.
   */
  static class WideNestedTable implements ScannableTable {
    private final int subfields;

    WideNestedTable(int subfields) {
      this.subfields = subfields;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      RelDataType varchar = factory.createSqlType(SqlTypeName.VARCHAR);
      RelDataType sub = factory.createStructType(List.of(varchar), List.of("X"));
      RelDataTypeFactory.Builder builder =
          factory
              .builder()
              .add("ID", SqlTypeName.INTEGER)
              .add("ATTRS", factory.createStructType(List.of(sub), List.of("SUB")))
              .add("ATTRS.SUB", sub)
              .add("LONE.X", SqlTypeName.VARCHAR)
              .add("ATTRS.SUB.X", SqlTypeName.VARCHAR);
      for (int i = 0; i < subfields; i++) {
        builder.add("ATTRS.F" + i, SqlTypeName.VARCHAR);
      }
      return builder.add("NAME", SqlTypeName.VARCHAR).build();
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(ImmutableList.of());
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
