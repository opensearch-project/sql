/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
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
 * Coercing an object or array field to a scalar has no valid implementation: the generated code
 * fails to compile with {@code Cannot cast "java.util.Map" to "java.lang.String"}. Verifies the
 * places where PPL coerces implicitly (chart and timechart split field) or explicitly ({@code
 * cast}) reject such a field with a client error instead.
 */
public class CalcitePPLNonScalarCoercionTest extends CalcitePPLAbstractTest {

  public CalcitePPLNonScalarCoercionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    ImmutableList<Object[]> rows =
        ImmutableList.of(
            new Object[] {0L, "request served", Map.of("name", "pod-a"), List.of("x")},
            new Object[] {60000L, "request served", Map.of("name", "pod-b"), List.of("y")});
    schema.add("app_logs", new ObjectFieldTable(rows));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void timechartByObjectFieldIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=app_logs | timechart span=1m count() by `dimensions.pod`"));
    assertTrue(
        e.getMessage(),
        e.getMessage().equals("Cannot chart by [dimensions.pod] because it is an object."));
  }

  @Test
  public void chartByObjectFieldIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=app_logs | chart count() over message by `dimensions.pod`"));
    assertTrue(
        e.getMessage(),
        e.getMessage().equals("Cannot chart by [dimensions.pod] because it is an object."));
  }

  @Test
  public void chartByArrayFieldIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=app_logs | chart count() over message by tags"));
    assertTrue(
        e.getMessage(),
        e.getMessage().equals("Cannot chart by [tags] because it holds multiple values."));
  }

  @Test
  public void castObjectFieldIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=app_logs | eval s = cast(`dimensions.pod` as string)"));
    assertEquals("Cannot cast an object to STRING", e.getMessage());
  }

  @Test
  public void castArrayFieldIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=app_logs | eval s = cast(tags as int)"));
    assertEquals("Cannot cast an array to INT", e.getMessage());
  }

  @Test
  public void castScalarFieldStillWorks() {
    verifyResultCount(getRelNode("source=app_logs | eval s = cast(message as int) | fields s"), 2);
  }

  /**
   * limit=0 skips the top-N pivot, so the split field is never coerced to a string. The quoted
   * column name is a pre-existing chart naming quirk, unrelated to this guard.
   */
  @Test
  public void chartWithoutLimitKeepsObjectField() {
    verifyLogical(
        getRelNode("source=app_logs | chart limit=0 count() over message by `dimensions.pod`"),
        "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalAggregate(group=[{0, 1}], count()=[COUNT()])\n"
            + "    LogicalProject(message=[$1], `dimensions.pod`=[$2])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "        LogicalTableScan(table=[[scott, app_logs]])\n");
  }

  @RequiredArgsConstructor
  public static class ObjectFieldTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("@timestamp", SqlTypeName.TIMESTAMP)
                .nullable(true)
                .add("message", SqlTypeName.VARCHAR)
                .nullable(true)
                .add(
                    "dimensions.pod",
                    factory.createTypeWithNullability(
                        factory.createMapType(
                            factory.createSqlType(SqlTypeName.VARCHAR),
                            factory.createSqlType(SqlTypeName.ANY)),
                        true))
                .add(
                    "tags",
                    factory.createTypeWithNullability(
                        factory.createArrayType(factory.createSqlType(SqlTypeName.VARCHAR), -1),
                        true))
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
      return Statistics.of(2d, ImmutableList.of(), RelCollations.createSingleton(0));
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
