/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
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
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.VectorErrors;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Vector columns ({@link ExprVectorType}) through the Calcite PPL lowering. */
public class CalcitePPLVectorTest extends CalcitePPLAbstractTest {

  private static final ExprVectorType VECTOR_3 =
      TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true);
  private static final ExprVectorType VECTOR_2 =
      TYPE_FACTORY.createVectorType(2, VectorDataType.FLOAT, "l2", true);

  public CalcitePPLVectorTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    ImmutableList<Object[]> vectors =
        ImmutableList.of(
            new Object[] {1, List.of(0.5f, 1.0f, 1.5f)},
            new Object[] {2, List.of(2.0f, 2.5f, 3.0f)},
            new Object[] {3, null});
    schema.add("VECS", new RowsTable(vectors, f -> vectorRow(VECTOR_3)));
    schema.add("VECS_COPY", new RowsTable(vectors, f -> vectorRow(VECTOR_3)));
    // Same doc_ids 1 and 2 as VECS with other vectors, to show which side a join keeps.
    schema.add(
        "VECS_RIGHT",
        new RowsTable(
            ImmutableList.of(
                new Object[] {1, List.of(9.0f, 9.0f, 9.0f)},
                new Object[] {2, List.of(8.0f, 8.0f, 8.0f)}),
            f -> vectorRow(VECTOR_3)));
    // Shares only the vector column with VECS.
    schema.add(
        "EMB_ONLY",
        new RowsTable(
            ImmutableList.of(new Object[] {"x", List.of(0.5f, 1.0f, 1.5f)}),
            f ->
                TYPE_FACTORY.createStructType(
                    List.of(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true), VECTOR_3),
                    List.of("label", "embedding"),
                    false)));
    schema.add(
        "VECS_2D",
        new RowsTable(
            ImmutableList.of(new Object[] {1, List.of(1.0f, 2.0f)}), f -> vectorRow(VECTOR_2)));
    schema.add(
        "VECS_TEXT",
        new RowsTable(
            ImmutableList.of(new Object[] {1, "a b c", List.of(0.5f, 1.0f, 1.5f)}),
            f ->
                TYPE_FACTORY.createStructType(
                    List.of(
                        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, true),
                        TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true),
                        VECTOR_3),
                    List.of("doc_id", "msg", "embedding"),
                    false)));
    schema.add(
        "DOCS",
        new RowsTable(
            ImmutableList.of(
                new Object[] {1, "first", "a"},
                new Object[] {2, "second", "b"},
                new Object[] {4, "fourth", "a"}),
            f ->
                f.builder()
                    .add("doc_id", SqlTypeName.INTEGER)
                    .nullable(true)
                    .add("title", SqlTypeName.VARCHAR)
                    .nullable(true)
                    .add("category", SqlTypeName.VARCHAR)
                    .nullable(true)
                    .build()));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  private static RelDataType vectorRow(ExprVectorType vectorType) {
    return TYPE_FACTORY.createStructType(
        List.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, true), vectorType),
        List.of("doc_id", "embedding"),
        false);
  }

  private static RelDataType fieldType(RelNode root, String name) {
    return root.getRowType().getField(name, true, false).getType();
  }

  private void assertNotComparable(String ppl) {
    SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> getRelNode(ppl));
    assertEquals(VectorErrors.VECTOR_NOT_COMPARABLE_MESSAGE, e.getMessage());
  }

  // ---------------------------------------------------------------- allowed operations

  @Test
  public void testProjectVector() {
    RelNode root = getRelNode("source=VECS | fields embedding, doc_id");
    verifyLogical(
        root,
        "LogicalProject(embedding=[$1], doc_id=[$0])\n"
            + "  LogicalTableScan(table=[[scott, VECS]])\n");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
    verifyResult(
        root,
        "embedding=[0.5, 1.0, 1.5]; doc_id=1\n"
            + "embedding=[2.0, 2.5, 3.0]; doc_id=2\n"
            + "embedding=null; doc_id=3\n");
    verifyPPLToSparkSQL(root, "SELECT `embedding`, `doc_id`\nFROM `scott`.`VECS`");
  }

  @Test
  public void testRenameAndEvalKeepVectorType() {
    RelNode root = getRelNode("source=VECS | rename embedding as v | eval w = v | fields w");
    assertEquals(VECTOR_3, fieldType(root, "w"));
  }

  @Test
  public void testArrayFunctionsReadVectors() {
    assertEquals(
        SqlTypeName.REAL,
        fieldType(getRelNode("source=VECS | eval x = mvindex(embedding, 0) | fields x"), "x")
            .getSqlTypeName());
    assertEquals(
        SqlTypeName.INTEGER,
        fieldType(getRelNode("source=VECS | eval x = array_length(embedding) | fields x"), "x")
            .getSqlTypeName());
    assertEquals(
        VECTOR_3,
        fieldType(
            getRelNode("source=VECS | eval x = coalesce(embedding, embedding) | fields x"), "x"));
    verifyResult(
        getRelNode("source=VECS | where doc_id = 1 | eval t = typeof(embedding) | fields t"),
        "t=VECTOR\n");
  }

  @Test
  public void testIsNullAndIsNotNullOnVector() {
    RelNode root = getRelNode("source=VECS | where isnotnull(embedding) | fields doc_id");
    verifyLogical(
        root,
        "LogicalProject(doc_id=[$0])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, VECS]])\n");
    verifyResult(root, "doc_id=1\ndoc_id=2\n");
    verifyResult(getRelNode("source=VECS | where isnull(embedding) | fields doc_id"), "doc_id=3\n");
  }

  @Test
  public void testJoinPassesVectorThrough() {
    RelNode root =
        getRelNode(
            "source=VECS | join left=v right=d on v.doc_id = d.doc_id DOCS"
                + " | fields d.title, v.embedding");
    verifyLogical(
        root,
        "LogicalProject(title=[$3], embedding=[$1])\n"
            + "  LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalTableScan(table=[[scott, DOCS]])\n");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
    verifyResult(
        root, "title=first; embedding=[0.5, 1.0, 1.5]\ntitle=second; embedding=[2.0, 2.5, 3.0]\n");
  }

  @Test
  public void testLeftJoinMakesVectorNullable() {
    RelNode root =
        getRelNode(
            "source=DOCS | left join left=d right=v on d.doc_id = v.doc_id VECS"
                + " | fields d.doc_id, v.embedding");
    RelDataType type = fieldType(root, "embedding");
    assertTrue(OpenSearchTypeFactory.isVectorType(type));
    assertEquals(3, ((ExprVectorType) type).getDimension());
    assertTrue(type.isNullable());
  }

  @Test
  public void testLookupPassesVectorThrough() {
    RelNode root = getRelNode("source=DOCS | lookup VECS doc_id | fields doc_id, embedding");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
  }

  @Test
  public void testAppendKeepsVectorTypeOfSameDimension() {
    RelNode root = getRelNode("source=VECS | append [ source=VECS_COPY ]");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
    verifyResultCount(root, 6);
  }

  @Test
  public void testAppendWithBranchWithoutTheVectorKeepsType() {
    RelNode root = getRelNode("source=VECS | append [ source=DOCS ]");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
  }

  @Test
  public void testUnionKeepsVectorTypeOfSameDimension() {
    RelNode root = getRelNode("| union [ search source=VECS ] [ search source=VECS_COPY ]");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
  }

  @Test
  public void testMultisearchKeepsVectorTypeOfSameDimension() {
    RelNode root =
        getRelNode(
            "| multisearch [ search source=VECS | where doc_id = 1 ]"
                + " [ search source=VECS_COPY | where doc_id = 2 ]");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
  }

  // ---------------------------------------------------------------- mixed vector types

  @Test
  public void testAppendOfDifferentDimensionsIsRejected() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> getRelNode("source=VECS | append [ source=VECS_2D ]"));
    assertEquals("cannot combine VECTOR(3, FLOAT) and VECTOR(2, FLOAT)", e.getMessage());
  }

  @Test
  public void testUnionOfDifferentDimensionsIsRejected() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> getRelNode("| union [ search source=VECS ] [ search source=VECS_2D ]"));
    assertEquals("cannot combine VECTOR(3, FLOAT) and VECTOR(2, FLOAT)", e.getMessage());
  }

  @Test
  public void testUnionOfVectorAndStringIsRejected() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                getRelNode(
                    "| union [ search source=VECS ]"
                        + " [ search source=DOCS | eval embedding = title"
                        + " | fields doc_id, embedding ]"));
    assertEquals("cannot combine VECTOR(3, FLOAT) and VARCHAR", e.getMessage());
  }

  // ---------------------------------------------------------------- vector not comparable

  @Test
  public void testCompareIsRejected() {
    assertNotComparable("source=VECS | where embedding = embedding");
    assertNotComparable("source=VECS | where embedding != embedding");
    assertNotComparable("source=VECS | where embedding < embedding");
    assertNotComparable("source=VECS | eval x = embedding >= embedding");
  }

  @Test
  public void testJoinOnVectorIsRejected() {
    assertNotComparable("source=VECS | join left=a right=b on a.embedding = b.embedding VECS_COPY");
  }

  @Test
  public void testJoinKeyNamedAsFieldIsRejected() {
    assertNotComparable("source=VECS | join embedding VECS_COPY");
    assertNotComparable("source=VECS | join doc_id, embedding VECS_COPY");
    assertNotComparable("source=VECS | join on embedding VECS_COPY");
    assertNotComparable("source=VECS | join type=inner max=1 embedding VECS_COPY");
  }

  /** Without keys, every shared column is a key, so a shared vector would be compared. */
  @Test
  public void testImplicitJoinWithSharedVectorIsRejected() {
    for (String ppl :
        List.of(
            "source=VECS | join VECS_COPY",
            "source=VECS | join type=left VECS_RIGHT",
            // The vector is the only shared column: no silent cross join.
            "source=EMB_ONLY | join VECS")) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> getRelNode(ppl));
      assertEquals(
          VectorErrors.VECTOR_NOT_COMPARABLE_MESSAGE
              + ": [embedding] is in both inputs of a join without keys, which would make it a"
              + " join key. Remove it from one input with `fields - embedding`, or name the join"
              + " keys",
          e.getMessage());
    }
  }

  @Test
  public void testImplicitJoinWithoutSharedVectorStillWorks() {
    RelNode root = getRelNode("source=VECS | fields - embedding | join VECS_RIGHT");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
    verifyResult(
        root, "doc_id=1; embedding=[9.0, 9.0, 9.0]\ndoc_id=2; embedding=[8.0, 8.0, 8.0]\n");
  }

  /**
   * With explicit keys, a vector both sides have follows the overwrite rule like any shared column:
   * by default the right input's value replaces the left one (null for an unmatched row of a left
   * join); with overwrite=false the left one is kept.
   */
  @Test
  public void testExplicitKeysKeepOneSideOfASharedVector() {
    verifyResult(
        getRelNode("source=VECS | join doc_id VECS_RIGHT | fields doc_id, embedding"),
        "doc_id=1; embedding=[9.0, 9.0, 9.0]\ndoc_id=2; embedding=[8.0, 8.0, 8.0]\n");
    verifyResult(
        getRelNode(
            "source=VECS | join type=left doc_id VECS_RIGHT | sort doc_id | fields doc_id,"
                + " embedding"),
        // Unmatched doc 3 takes the right input's nulls for every shared column, the key too.
        "doc_id=null; embedding=null\n"
            + "doc_id=1; embedding=[9.0, 9.0, 9.0]\n"
            + "doc_id=2; embedding=[8.0, 8.0, 8.0]\n");
    verifyResult(
        getRelNode(
            "source=VECS | join overwrite=false doc_id VECS_RIGHT | sort doc_id | fields doc_id,"
                + " embedding"),
        "doc_id=1; embedding=[0.5, 1.0, 1.5]\ndoc_id=2; embedding=[2.0, 2.5, 3.0]\n");
    assertEquals(
        VECTOR_3,
        fieldType(getRelNode("source=VECS | join type=left doc_id VECS_RIGHT"), "embedding"));
  }

  @Test
  public void testXyseriesOnVectorIsRejected() {
    // x-field: grouped and sorted
    assertNotComparable("source=VECS_TEXT | xyseries embedding msg in (\"a\") doc_id");
    // y-data field: aggregated with MAX
    assertNotComparable("source=VECS_TEXT | xyseries doc_id msg in (\"a\") embedding");
    // y-name field: matched against the pivot values
    assertNotComparable("source=VECS_TEXT | xyseries doc_id embedding in (\"a\") doc_id");
    getRelNode("source=VECS_TEXT | xyseries doc_id msg in (\"a\") doc_id");
  }

  @Test
  public void testLookupOnVectorKeyIsRejected() {
    assertNotComparable("source=VECS | lookup VECS_COPY embedding");
    assertNotComparable("source=VECS | lookup VECS_COPY embedding as embedding");
  }

  @Test
  public void testInSubqueryIsRejected() {
    assertNotComparable("source=VECS | where embedding in [ source=VECS_COPY | fields embedding ]");
    // A non-vector left side compared with a vector subquery column.
    assertNotComparable("source=VECS | where doc_id in [ source=VECS_COPY | fields embedding ]");
  }

  @Test
  public void testInSubqueryOnOtherColumnStillWorks() {
    RelNode root =
        getRelNode(
            "source=VECS | where doc_id in [ source=VECS_COPY | where doc_id < 3 | fields doc_id ]"
                + " | fields embedding");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
  }

  @Test
  public void testStreamstatsByVectorIsRejected() {
    assertNotComparable("source=VECS | streamstats count() by embedding");
    assertNotComparable("source=VECS | streamstats reset_before=doc_id>1 count() by embedding");
    assertNotComparable("source=VECS | streamstats reset_after=doc_id>1 count() by embedding");
    assertNotComparable("source=VECS | streamstats global=true window=2 count() by embedding");
  }

  @Test
  public void testTrendlineSortOnVectorIsRejected() {
    assertNotComparable("source=VECS | trendline sort embedding sma(2, doc_id)");
    assertNotComparable("source=VECS | trendline sort - embedding sma(2, doc_id)");
  }

  @Test
  public void testOrderOrEqualityAggregatesAreRejected() {
    for (String agg :
        List.of(
            "min(embedding)",
            "max(embedding)",
            "dc(embedding)",
            "distinct_count(embedding)",
            "distinct_count_approx(embedding)",
            "values(embedding)")) {
      assertNotComparable("source=VECS | stats " + agg);
      assertNotComparable("source=VECS | stats " + agg + " by doc_id");
    }
    assertNotComparable("source=VECS | eventstats max(embedding)");
    assertNotComparable("source=VECS | eventstats dc(embedding) by doc_id");
    assertNotComparable("source=VECS | streamstats min(embedding)");
  }

  @Test
  public void testCollectingAggregatesAcceptVectors() {
    verifyResult(getRelNode("source=VECS | stats count(embedding) as c"), "c=2\n");
    for (String agg :
        // take() and list() reject any array argument through their own type checks.
        List.of("count(embedding)", "first(embedding)", "last(embedding)")) {
      getRelNode("source=VECS | stats " + agg + " as a by doc_id");
    }
    assertEquals(VECTOR_3, fieldType(getRelNode("source=VECS | stats first(embedding) as f"), "f"));
    getRelNode("source=VECS | eventstats count(embedding) as c");
  }

  @Test
  public void testPatternsByVectorIsRejected() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.PATTERN_METHOD);
    doReturn("label").when(settings).getSettingValue(Key.PATTERN_MODE);
    doReturn(10).when(settings).getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT);
    doReturn(100000).when(settings).getSettingValue(Key.PATTERN_BUFFER_LIMIT);
    doReturn(false).when(settings).getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN);
    // The simple_pattern label mode labels each row on its own and does not group by the by-list.
    getRelNode("source=VECS_TEXT | patterns msg by embedding");
    assertNotComparable("source=VECS_TEXT | patterns msg by embedding mode=aggregation");
    assertNotComparable("source=VECS_TEXT | patterns msg by embedding method=brain");
    assertNotComparable(
        "source=VECS_TEXT | patterns msg by embedding method=brain mode=aggregation");
  }

  /** mvcombine groups by every other column, so a vector among them must be removed first. */
  @Test
  public void testMvcombineWithVectorColumnIsRejected() {
    assertNotComparable("source=VECS_TEXT | mvcombine msg");
    getRelNode("source=VECS_TEXT | fields - embedding | mvcombine msg");
  }

  /** graphlookup matches start and connect-from values against the connect-to field. */
  @Test
  public void testGraphlookupOnVectorIsRejected() {
    assertNotComparable(
        "source=VECS | graphLookup VECS_COPY start=embedding edge=doc_id-->doc_id as path");
    assertNotComparable(
        "source=VECS | graphLookup VECS_COPY start=doc_id edge=embedding-->doc_id as path");
    assertNotComparable(
        "source=VECS | graphLookup VECS_COPY start=doc_id edge=doc_id-->embedding as path");
    assertEquals(
        VECTOR_3,
        fieldType(
                getRelNode(
                    "source=VECS | graphLookup VECS_COPY start=doc_id edge=doc_id-->doc_id as"
                        + " path"),
                "path")
            .getComponentType()
            .getField("embedding", true, false)
            .getType());
  }

  @Test
  public void testSortIsRejected() {
    assertNotComparable("source=VECS | sort embedding");
    assertNotComparable("source=VECS | sort - embedding");
    assertNotComparable("source=VECS | sort doc_id, embedding");
  }

  @Test
  public void testGroupByIsRejected() {
    assertNotComparable("source=VECS | stats count() by embedding");
    assertNotComparable("source=VECS | stats count() by doc_id, embedding");
    assertNotComparable("source=VECS | top embedding");
    assertNotComparable("source=VECS | rare embedding");
    assertNotComparable("source=VECS | eventstats count() by embedding");
  }

  @Test
  public void testDedupIsRejected() {
    assertNotComparable("source=VECS | dedup embedding");
    assertNotComparable("source=VECS | dedup doc_id, embedding");
  }

  @Test
  public void testComparingOtherColumnsStillWorks() {
    RelNode root = getRelNode("source=VECS | where doc_id > 1 | sort - doc_id | fields embedding");
    assertEquals(VECTOR_3, fieldType(root, "embedding"));
    verifyResult(root, "embedding=null\nembedding=[2.0, 2.5, 3.0]\n");
    assertFalse(OpenSearchTypeFactory.isVectorType(fieldType(getRelNode("source=DOCS"), "title")));
  }

  /** A scannable table with a fixed row type and rows. */
  private static class RowsTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;
    private final Function<RelDataTypeFactory, RelDataType> rowType;

    RowsTable(ImmutableList<Object[]> rows, Function<RelDataTypeFactory, RelDataType> rowType) {
      this.rows = rows;
      this.rowType = rowType;
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows.stream().map(r -> Arrays.copyOf(r, r.length)).toList());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType.apply(TYPE_FACTORY);
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
