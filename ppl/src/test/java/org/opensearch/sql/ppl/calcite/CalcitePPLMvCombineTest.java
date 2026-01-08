/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

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

public class CalcitePPLMvCombineTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvCombineTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    ImmutableList<Object[]> rows =
        ImmutableList.of(
            // existing "basic"
            new Object[] {"basic", "A", 10},
            new Object[] {"basic", "A", 20},
            new Object[] {"basic", "B", 60},
            new Object[] {"basic", "A", 30},

            // new: NULL target values case
            new Object[] {"nulls", "A", null},
            new Object[] {"nulls", "A", 10},
            new Object[] {"nulls", "B", null},

            // new: single-row case
            new Object[] {"single", "Z", 5});

    schema.add("MVCOMBINE_DATA", new MvCombineDataTable(rows));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testMvCombineBasic() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"basic\" "
            + "| fields case, ip, packets "
            + "| mvcombine packets "
            + "| sort ip";

    RelNode root = getRelNode(ppl);

    // NOTE: Your implementation casts COLLECT result (MULTISET) to ARRAY.
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(case=[$0], ip=[$1], packets=[CAST($2):INTEGER ARRAY NOT NULL])\n"
            + "    LogicalAggregate(group=[{0, 1}], packets=[COLLECT($2)])\n"
            + "      LogicalFilter(condition=[=($0, 'basic')])\n"
            + "        LogicalTableScan(table=[[scott, MVCOMBINE_DATA]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvCombineNomvWithCustomDelim() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"basic\" "
            + "| fields case, ip, packets "
            + "| mvcombine packets delim=\"|\" nomv=true "
            + "| sort ip";

    RelNode root = getRelNode(ppl);

    // NOTE: Your implementation does:
    //   COLLECT -> CAST to INTEGER ARRAY -> CAST to VARCHAR ARRAY -> ARRAY_TO_STRING(..., '|')
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(case=[$0], ip=[$1], packets=[ARRAY_TO_STRING(CAST(CAST($2):INTEGER"
            + " ARRAY NOT NULL):VARCHAR NOT NULL ARRAY NOT NULL, '|')])\n"
            + "    LogicalAggregate(group=[{0, 1}], packets=[COLLECT($2)])\n"
            + "      LogicalFilter(condition=[=($0, 'basic')])\n"
            + "        LogicalTableScan(table=[[scott, MVCOMBINE_DATA]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvCombineWithNullTargetValues() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"nulls\" "
            + "| fields case, ip, packets "
            + "| mvcombine packets "
            + "| sort ip";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(case=[$0], ip=[$1], packets=[CAST($2):INTEGER ARRAY NOT NULL])\n"
            + "    LogicalAggregate(group=[{0, 1}], packets=[COLLECT($2)])\n"
            + "      LogicalFilter(condition=[=($0, 'nulls')])\n"
            + "        LogicalTableScan(table=[[scott, MVCOMBINE_DATA]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvCombineNonExistentField() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"basic\" "
            + "| fields case, ip, packets "
            + "| mvcombine does_not_exist";

    Exception ex = assertThrows(Exception.class, () -> getRelNode(ppl));

    // Keep this loose: different layers may wrap exceptions.
    // We just need to prove the command fails for missing target field.
    String msg = String.valueOf(ex.getMessage());
    org.junit.Assert.assertTrue(
        "Expected error message to mention missing field. Actual: " + msg,
        msg.toLowerCase().contains("does_not_exist") || msg.toLowerCase().contains("field"));
  }

  @Test
  public void testMvCombineSingleRow() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"single\" "
            + "| fields case, ip, packets "
            + "| mvcombine packets "
            + "| sort ip";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(case=[$0], ip=[$1], packets=[CAST($2):INTEGER ARRAY NOT NULL])\n"
            + "    LogicalAggregate(group=[{0, 1}], packets=[COLLECT($2)])\n"
            + "      LogicalFilter(condition=[=($0, 'single')])\n"
            + "        LogicalTableScan(table=[[scott, MVCOMBINE_DATA]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvCombineEmptyResult() {
    String ppl =
        "source=MVCOMBINE_DATA "
            + "| where case = \"no_such_case\" "
            + "| fields case, ip, packets "
            + "| mvcombine packets "
            + "| sort ip";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(case=[$0], ip=[$1], packets=[CAST($2):INTEGER ARRAY NOT NULL])\n"
            + "    LogicalAggregate(group=[{0, 1}], packets=[COLLECT($2)])\n"
            + "      LogicalFilter(condition=[=($0, 'no_such_case')])\n"
            + "        LogicalTableScan(table=[[scott, MVCOMBINE_DATA]])\n";
    verifyLogical(root, expectedLogical);
  }

  // ========================================================================
  // Custom ScannableTable for deterministic mvcombine planning tests
  // ========================================================================

  @RequiredArgsConstructor
  static class MvCombineDataTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("case", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("ip", SqlTypeName.VARCHAR)
                .nullable(true)
                .add("packets", SqlTypeName.INTEGER)
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
