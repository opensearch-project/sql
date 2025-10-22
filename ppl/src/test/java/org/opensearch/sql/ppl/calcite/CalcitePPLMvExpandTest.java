/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

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

public class CalcitePPLMvExpandTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvExpandTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

    ImmutableList<Object[]> users =
        ImmutableList.of(
            // happy: multiple skills
            new Object[] {
              "happy",
              new Object[] {
                new Object[] {"python", null},
                new Object[] {"java", null},
                new Object[] {"sql", null}
              }
            },
            // single: single skill
            new Object[] {"single", new Object[] {new Object[] {"go", null}}},
            // empty: empty skills array
            new Object[] {"empty", new Object[] {}},
            // nullskills: null skills array
            new Object[] {"nullskills", null},
            // noskills: missing skills (simulate with null)
            new Object[] {"noskills", null},
            // missingattr: skills with missing fields
            new Object[] {
              "missingattr",
              new Object[] {
                new Object[] {"c", null},
                new Object[] {null, "advanced"}
              }
            },
            // complex: skills with some missing name/level
            new Object[] {
              "complex",
              new Object[] {
                new Object[] {"ml", "expert"},
                new Object[] {"ai", null},
                new Object[] {null, "novice"}
              }
            },
            // duplicate: skills with duplicate names
            new Object[] {
              "duplicate",
              new Object[] {
                new Object[] {"dup", null},
                new Object[] {"dup", null}
              }
            },
            // large: skills with many elements
            new Object[] {
              "large",
              new Object[] {
                new Object[] {"s1", null}, new Object[] {"s2", null}, new Object[] {"s3", null},
                new Object[] {"s4", null}, new Object[] {"s5", null}, new Object[] {"s6", null},
                new Object[] {"s7", null}, new Object[] {"s8", null}, new Object[] {"s9", null},
                new Object[] {"s10", null}
              }
            },
            // primitive: array of primitives instead of objects
            new Object[] {"primitive", new Object[] {"python", "java"}},
            // allnulls: array of nulls
            new Object[] {"allnulls", new Object[] {null, null}},
            // emptyobj: array with an empty object
            new Object[] {"emptyobj", new Object[] {new Object[] {}}},
            // deeplyNested: array of arrays
            new Object[] {
              "deeplyNested",
              new Object[] {
                new Object[] {new Object[] {"python", null}},
                new Object[] {new Object[] {"java", null}}
              }
            },
            // mixedTypes: array with mixed types
            new Object[] {
              "mixedTypes", new Object[] {"python", 42, true, null, new Object[] {"java", null}}
            },
            // nestedObject: array of objects with objects as attributes
            new Object[] {
              "nestedObject",
              new Object[] {
                new Object[] {new Object[] {"meta", new Object[] {"name", "python", "years", 5}}}
              }
            },
            // allEmptyObjects: array of empty objects
            new Object[] {"allEmptyObjects", new Object[] {new Object[] {}, new Object[] {}}},
            // allEmptyArrays: array of empty arrays
            new Object[] {"allEmptyArrays", new Object[] {new Object[] {}, new Object[] {}}},
            // arrayOfArraysOfPrimitives
            new Object[] {
              "arrayOfArraysOfPrimitives",
              new Object[] {new Object[] {"python", "java"}, new Object[] {"sql"}}
            },
            // specialValues: array with Infinity, NaN, very long string, unicode
            new Object[] {
              "specialValues",
              new Object[] {
                Double.POSITIVE_INFINITY,
                Double.NaN,
                "😀😃😄😁",
                new String(new char[10000]).replace('\0', 'x')
              }
            });
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
      assertNotNull(root);
    } catch (Exception e) {
      fail("mvexpand on array with special values should not throw, but got: " + e.getMessage());
    }
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
