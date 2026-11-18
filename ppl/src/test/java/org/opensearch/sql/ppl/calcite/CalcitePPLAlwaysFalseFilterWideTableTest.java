/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.common.setting.Settings;

/**
 * Regression tests for opensearch-project/sql#5952: a filter that Calcite simplifies to always
 * false is replaced by an empty {@code Values}. Calcite's collation metadata for it is O(n^2) in
 * the number of fields (O(n^3) with a projection above), so on a wide index planning took minutes
 * and exhausted the heap. With the fix the empty {@code Values} carries no collations and planning
 * is independent of the field count.
 */
public class CalcitePPLAlwaysFalseFilterWideTableTest extends CalcitePPLAbstractTest {

  private static final int FIELD_COUNT = 4000;
  // Before the fix, each of these queries allocated gigabytes and took seconds to minutes.
  private static final long TIMEOUT_MS = 30_000;
  // Each query takes well under 100 ms with the fix, and several seconds without it.
  private static final long MAX_PLAN_AND_RUN_MS = 1_000;

  public CalcitePPLAlwaysFalseFilterWideTableTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Before
  public void setUpRex() {
    doReturn(10).when(settings).getSettingValue(Settings.Key.PPL_REX_MAX_MATCH_LIMIT);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("WIDE", new WideTable(FIELD_COUNT));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test(timeout = TIMEOUT_MS)
  public void testLiteralComparedToLiteralWithEval() {
    RelNode root = getRelNode("source=WIDE | where 'a' = 'b' | eval x = 1 | fields x | head 10");
    verifyLogical(root, "LogicalSort(fetch=[10])\n  LogicalValues(tuples=[[]])\n");
    assertEmptyValuesHasNoCollation(root);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testNumericFieldNotEqualEmptyStringWithRex() {
    // `code` is numeric, so `code != ''` can never be true.
    RelNode root =
        getRelNode(
            "source=WIDE | where isnotnull(code) and code != '' | rex field=status"
                + " '(?<p1>a)' | rex field=status '(?<p2>b)' | sort status | head 10");
    assertEmptyValuesHasNoCollation(root);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testFiltersThatContradictOnlyAfterMerge() {
    // The visitor keeps the two filters; FilterMergeRule in CalciteToolsHelper.optimize merges
    // them into an always-false condition.
    RelNode root =
        CalciteToolsHelper.optimize(
            getRelNodeRaw("source=WIDE | where status = 'a' | where status = 'b' | head 10"), null);
    assertEmptyValuesHasNoCollation(root);
  }

  /**
   * All fields are top-level, so the empty {@code Values} keeps every field in the output. The
   * Volcano planner then prunes it and converts it to {@code EnumerableValues}; both steps used to
   * derive O(n^2) collations again (several seconds at 4,000 fields).
   */
  @Test(timeout = TIMEOUT_MS)
  public void testWideOutputThroughProductionPlanner() {
    // Run each query once to warm up code generation, then time the second run.
    for (String ppl :
        List.of(
            "source=WIDE | where 'a' = 'b' | head 10",
            "source=WIDE | where status = 'a' | where status = 'b' | eval x = 1 | head 10")) {
      assertEquals(0, runWithProductionRunner(ppl));
      long start = System.nanoTime();
      assertEquals(0, runWithProductionRunner(ppl));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(ppl + " took " + elapsedMs + " ms", elapsedMs < MAX_PLAN_AND_RUN_MS);
    }
  }

  private static void assertEmptyValuesHasNoCollation(RelNode root) {
    Values values = findValues(root);
    assertTrue("expected an empty Values in:\n" + root.explain(), values != null);
    assertTrue(values.getTuples().isEmpty());
    assertTrue(
        values.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE) == null
            || values
                .getTraitSet()
                .getTraits(RelCollationTraitDef.INSTANCE)
                .equals(List.of(RelCollations.EMPTY)));
    assertTrue(values.getCluster().getMetadataQuery().collations(values).isEmpty());
  }

  private static Values findValues(RelNode node) {
    if (node instanceof Values) {
      return (Values) node;
    }
    for (RelNode input : node.getInputs()) {
      Values found = findValues(input.stripped());
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** A table with {@code status}, {@code code} and {@code fieldCount - 2} keyword-like fields. */
  private static class WideTable extends AbstractTable implements ScannableTable {
    private final int fieldCount;

    WideTable(int fieldCount) {
      this.fieldCount = fieldCount;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder =
          typeFactory
              .builder()
              .add("status", SqlTypeName.VARCHAR)
              .nullable(true)
              .add("code", SqlTypeName.BIGINT)
              .nullable(true);
      for (int i = 0; i < fieldCount - 2; i++) {
        builder.add("f" + i, SqlTypeName.VARCHAR).nullable(true);
      }
      return builder.build();
    }
  }
}
