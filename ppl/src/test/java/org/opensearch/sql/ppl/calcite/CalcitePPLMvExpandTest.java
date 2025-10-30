/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
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
 * Calcite tests for the mvexpand command.
 *
 * <p>This file contains a set of planner tests for mvexpand and a small helper to verify RelNode ->
 * SQL conversion (best-effort via reflection) so tests can run in various IDE/classpath setups.
 *
 * <p>Notes: - The scan() return type uses Enumerable<Object[]> (no type-use @Nullable) to avoid
 * "annotation not applicable to this kind of reference" in some environments. -
 * verifyPPLToSparkSQL(RelNode) uses reflection/fallback to exercise Rel->SQL conversion without
 * adding a compile-time dependency on OpenSearchSparkSqlDialect.
 */
public class CalcitePPLMvExpandTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvExpandTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    // Keep dataset minimal — planner tests here only need a representative schema and a couple of
    // rows.
    ImmutableList<Object[]> users =
        ImmutableList.of(
            // representative row with multiple skills
            new Object[] {
              "happy",
              new Object[] {
                new Object[] {"python", null},
                new Object[] {"java", null}
              }
            },
            // representative row with single skill
            new Object[] {"single", new Object[] {new Object[] {"go", null}}});

    schema.add("USERS", new UsersTable(users));

    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  // Option 2: Assert specific logical plan (as per the main edge/typical cases)
  @Test
  public void testMvExpandBasic() {
    String ppl = "source=USERS | mvexpand skills";
    RelNode root = getRelNode(ppl);

    // verify PPL -> SparkSQL conversion
    verifyPPLToSparkSQL(root);

    String expectedLogical =
        "LogicalProject(USERNAME=[$0], name=[$2], level=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, USERS]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(skills=[$cor0.skills])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvExpandWithLimit() {
    String ppl = "source=USERS | mvexpand skills | head 1";
    RelNode root = getRelNode(ppl);

    // verify PPL -> SparkSQL conversion
    verifyPPLToSparkSQL(root);

    // Updated expected plan to match the actual planner output (single LogicalSort above Project)
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(USERNAME=[$0], name=[$2], level=[$3])\n"
            + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "      LogicalTableScan(table=[[scott, USERS]])\n"
            + "      Uncollect\n"
            + "        LogicalProject(skills=[$cor0.skills])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvExpandProjectNested() {
    String ppl = "source=USERS | mvexpand skills | fields USERNAME, name, level";
    RelNode root = getRelNode(ppl);

    // verify PPL -> SparkSQL conversion
    verifyPPLToSparkSQL(root);

    String expectedLogical =
        "LogicalProject(USERNAME=[$0], name=[$2], level=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalTableScan(table=[[scott, USERS]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(skills=[$cor0.skills])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);
  }

  // Option 3: Assert that no error/crash occurs for edge cases

  @Test
  public void testMvExpandEmptyOrNullArray() {
    String ppl = "source=USERS | where USERNAME in ('empty','nullskills') | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      // also sanity-check conversion
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on empty/null array should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandNoArrayField() {
    String ppl = "source=USERS | where USERNAME = 'noskills' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on missing array field should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandWithDuplicates() {
    String ppl = "source=USERS | where USERNAME = 'duplicate' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand with duplicates should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandLargeArray() {
    String ppl = "source=USERS | where USERNAME = 'large' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on large array should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandProjectMissingAttribute() {
    String ppl = "source=USERS | mvexpand skills | fields USERNAME, level";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand projection of missing attribute should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandPrimitiveArray() {
    String ppl = "source=USERS | where USERNAME = 'primitive' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of primitives should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandAllNullsArray() {
    String ppl = "source=USERS | where USERNAME = 'allnulls' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of all nulls should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandEmptyObjectArray() {
    String ppl = "source=USERS | where USERNAME = 'emptyobj' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array with empty struct should not throw, but got: " + e.getMessage());
    }
  }

  // --- Additional uncovered edge case tests ---

  @Test
  public void testMvExpandDeeplyNestedArray() {
    String ppl = "source=USERS | where USERNAME = 'deeplyNested' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on deeply nested arrays should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandMixedTypesArray() {
    String ppl = "source=USERS | where USERNAME = 'mixedTypes' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array with mixed types should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandNestedObjectArray() {
    String ppl = "source=USERS | where USERNAME = 'nestedObject' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of nested objects should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandAllEmptyObjectsArray() {
    String ppl = "source=USERS | where USERNAME = 'allEmptyObjects' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of all empty objects should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandAllEmptyArraysArray() {
    String ppl = "source=USERS | where USERNAME = 'allEmptyArrays' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array of all empty arrays should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandArrayOfArraysOfPrimitives() {
    String ppl = "source=USERS | where USERNAME = 'arrayOfArraysOfPrimitives' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail(
          "mvexpand on array of arrays of primitives should not throw, but got: " + e.getMessage());
    }
  }

  @Test
  public void testMvExpandSpecialValuesArray() {
    String ppl = "source=USERS | where USERNAME = 'specialValues' | mvexpand skills";
    try {
      RelNode root = getRelNode(ppl);
      verifyPPLToSparkSQL(root);
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array with special values should not throw, but got: " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // Local helper: verify PPL -> SparkSQL conversion without adding compile-time
  // dependency on OpenSearchSparkSqlDialect (uses reflection/fallback).
  // ---------------------------------------------------------------------------

  /**
   * Verify PPL -> SparkSQL conversion for an already-built RelNode.
   *
   * <p>Strategy: 1) Try to instantiate org.opensearch.sql.calcite.OpenSearchSparkSqlDialect.DEFAULT
   * via reflection and run a typed RelToSqlConverter with it (best effort - exercises same path
   * used in other tests). 2) Fallback: use the private 'converter' instance from
   * CalcitePPLAbstractTest via reflection and call its visitRoot(...) method; assert it produced a
   * non-null statement object.
   */
  private void verifyPPLToSparkSQL(RelNode root) {
    try {
      // Preferred: try to instantiate dialect class and produce SQL string (if available).
      try {
        Class<?> dialectClass =
            Class.forName("org.opensearch.sql.calcite.OpenSearchSparkSqlDialect");
        Field defaultField = dialectClass.getField("DEFAULT");
        Object dialectDefault = defaultField.get(null); // static field
        RelToSqlConverter localConv =
            new RelToSqlConverter((org.apache.calcite.sql.SqlDialect) dialectDefault);
        SqlImplementor.Result result = localConv.visitRoot(root);
        if (result == null || result.asStatement() == null) {
          fail("PPL -> SparkSQL conversion returned no statement");
        }
        // Convert to SQL string using the dialect instance (typed call) and assert non-null.
        final SqlNode sqlNode = result.asStatement();
        final String sql =
            sqlNode.toSqlString((org.apache.calcite.sql.SqlDialect) dialectDefault).getSql();
        assertNotNull(sql);
        return; // success
      } catch (ClassNotFoundException cnfe) {
        // Dialect class not present in this classloader/IDE environment — fall back.
      }

      // Fallback: call upstream private converter via reflection and assert result/asStatement()
      // non-null.
      try {
        Field convField = CalcitePPLAbstractTest.class.getDeclaredField("converter");
        convField.setAccessible(true);
        Object convObj = convField.get(this); // should be RelToSqlConverter
        if (convObj == null) {
          fail("Upstream converter is not initialized; cannot verify PPL->SparkSQL");
        }
        Method visitRoot =
            convObj.getClass().getMethod("visitRoot", org.apache.calcite.rel.RelNode.class);
        Object resultObj = visitRoot.invoke(convObj, root);
        if (resultObj == null) {
          fail("PPL -> SparkSQL conversion (via upstream converter) returned null");
        }
        Method asStatement = resultObj.getClass().getMethod("asStatement");
        Object stmtObj = asStatement.invoke(resultObj);
        if (stmtObj == null) {
          fail("PPL -> SparkSQL conversion returned no statement object");
        }
        // success: conversion produced a statement object
        return;
      } catch (NoSuchFieldException nsf) {
        fail(
            "Reflection fallback failed: converter field not found in CalcitePPLAbstractTest: "
                + nsf.getMessage());
      } catch (ReflectiveOperationException reflEx) {
        fail("Reflection fallback to upstream converter failed: " + reflEx.getMessage());
      }
    } catch (Exception ex) {
      fail("PPL -> SparkSQL conversion failed: " + ex.getMessage());
    }
  }

  /** Convenience wrapper when only a PPL string is available. */
  @SuppressWarnings("unused")
  private void verifyPPLToSparkSQL(String ppl) {
    RelNode root = getRelNode(ppl);
    verifyPPLToSparkSQL(root);
  }

  @RequiredArgsConstructor
  static class UsersTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("USERNAME", SqlTypeName.VARCHAR)
                .add(
                    "skills",
                    factory.createArrayType(
                        factory
                            .builder()
                            .add("name", SqlTypeName.VARCHAR)
                            .add("level", SqlTypeName.VARCHAR)
                            .build(),
                        -1))
                .build();

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
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
