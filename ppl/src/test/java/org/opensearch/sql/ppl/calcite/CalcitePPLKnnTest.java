/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.apache.calcite.tools.RelRunners;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Knn;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.calcite.knn.KnnPlanBuilder;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansCommandContext;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * The {@code knn} command through the real PPL parser, for what only the parser builds: the
 * projections it puts around join inputs and subsearches, which strip metadata columns, and the
 * default output. There is no grammar for {@code knn} yet, so in these queries {@code kmeans
 * centroids=N} stands for {@code knn field=embedding vector=[0.5, 1.0, 1.5] k=N} (and a bare {@code
 * kmeans} for the same without k).
 */
public class CalcitePPLKnnTest extends CalcitePPLAbstractTest {

  private final DataSourceService dataSourceService = mock(DataSourceService.class);

  public CalcitePPLKnnTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add(
        "VECS",
        new MetadataTable(
            ImmutableList.of(
                new Object[] {1, List.of(0.5f, 1.0f, 1.5f)},
                new Object[] {2, List.of(2.0f, 2.5f, 3.0f)}),
            List.of("doc_id", "embedding"),
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true))));
    schema.add(
        "OBJS",
        new MetadataTable(
            ImmutableList.of(new Object[] {1, List.of(0.5f, 1.0f, 1.5f), 7, 8}),
            List.of("doc_id", "embedding", "obj._score", "obj.b"),
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true),
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))));
    schema.add(
        "S0",
        new MetadataTable(
            ImmutableList.of(new Object[] {1, List.of(0.5f, 1.0f, 1.5f), 9}),
            List.of("doc_id", "embedding", "_score0"),
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true),
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))));
    schema.add(
        "DOCS",
        new MetadataTable(
            ImmutableList.of(new Object[] {1, "first"}, new Object[] {2, "second"}),
            List.of("doc_id", "title"),
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /** Lower a query in which {@code kmeans} stands for the knn command. */
  private RelNode knnPlan(String ppl) {
    doReturn(false).when(dataSourceService).dataSourceExists(any());
    AstBuilder astBuilder =
        new AstBuilder(ppl, settings) {
          @Override
          public UnresolvedPlan visitKmeansCommand(KmeansCommandContext ctx) {
            Kmeans kmeans = (Kmeans) super.visitKmeansCommand(ctx);
            List<Argument> options = new ArrayList<>();
            Literal centroids = kmeans.getArguments().get("centroids");
            if (centroids != null) {
              options.add(AstDSL.argument("k", centroids));
            }
            return new Knn(AstDSL.field("embedding"), AstDSL.vectorLiteral(0.5, 1.0, 1.5), options);
          }
        };
    Query query =
        (Query)
            new AstStatementBuilder(
                    astBuilder, AstStatementBuilder.StatementBuilderContext.builder().build())
                .visit(pplParser.parse(ppl));
    // Lower the same AST twice, in fresh contexts and with one visitor, as the profiling path of
    // QueryService does; lowering changes the AST, so the second pass must give the same plan.
    CalciteRelNodeVisitor visitor = new CalciteRelNodeVisitor(dataSourceService);
    RelNode first = lowerOnce(visitor, query.getPlan());
    RelNode second = lowerOnce(visitor, query.getPlan());
    assertEquals("lowering the same AST again differs", first.explain(), second.explain());
    return first;
  }

  private RelNode lowerOnce(CalciteRelNodeVisitor visitor, UnresolvedPlan plan) {
    CalcitePlanContext context = createBuilderContext();
    visitor.analyzeStatement(plan, context);
    return context.relBuilder.build();
  }

  private static long count(List<String> names, String name) {
    return names.stream().filter(name::equals).count();
  }

  @Test
  public void scoreIsVisibleByDefaultAndOtherMetadataIsNot() {
    RelNode root = knnPlan("source=VECS | kmeans centroids=2");
    verifyLogical(
        root,
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3])\n"
            + "  LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "      LogicalTableScan(table=[[scott, VECS]])\n");
    // Spark has no approximate nearest-neighbour search to translate KNN_SCORE to.
    SemanticCheckException e =
        assertThrows(SemanticCheckException.class, () -> verifyPPLToSparkSQL(root, ""));
    assertEquals(KnnErrors.NOT_TRANSPILABLE_MESSAGE, e.getMessage());
  }

  @Test
  public void derivedKFromAFollowingHead() {
    verifyLogical(
        knnPlan("source=VECS | where doc_id > 0 | kmeans | fields doc_id, _score | head 1"),
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(doc_id=[$0], _score=[$3])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[1])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL))])\n"
            + "        LogicalFilter(condition=[>($0, 0)])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  @Test
  public void missingKAfterAPostFilter() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | kmeans | where doc_id > 0 | head 10"));
    assertEquals(
        "knn requires k or a following head; k cannot be derived from a head after [where], set k"
            + " explicitly, e.g. k=10",
        e.getMessage());
  }

  /** Scenario 4 of the design: the vector index joined back to the documents. */
  @Test
  public void scoreSurvivesTheJoinInput() {
    verifyLogical(
        knnPlan(
            "source=VECS | kmeans centroids=10 | join left=v right=d on v.doc_id = d.doc_id DOCS"
                + " | fields d.title, v._score"),
        "LogicalProject(title=[$4], _score=[$2])\n"
            + "  LogicalJoin(condition=[=($0, $3)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 10))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  @Test
  public void withoutKnnTheJoinStillStripsScore() {
    List<String> names =
        getRelNode("source=DOCS | join left=d right=v on d.doc_id = v.doc_id VECS")
            .getRowType()
            .getFieldNames();
    assertEquals(names.toString(), 0, count(names, "_score"));
  }

  /**
   * A lookup without an output list: the k-NN _score ($3 of the left input) wins over DOCS's hidden
   * _score.
   */
  @Test
  public void lookupKeepsTheKnnScoreOverTheTableHiddenScore() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | lookup DOCS doc_id"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], title=[$5])\n"
            + "  LogicalJoin(condition=[=($0, $4)], joinType=[left])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1], _id=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** append, k-NN in the main search: the subsearch's rows get a null _score, not DOCS's own. */
  @Test
  public void appendGivesNullScoresToTheOtherInput() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | append [ source=DOCS ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], title=[$4])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[$3],"
            + " title=[null:VARCHAR])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[null:VECTOR(3, FLOAT, l2)], _id=[$2],"
            + " _score=[null:REAL], title=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** append, k-NN in the subsearch: the main rows get a null _score, not their hidden one. */
  @Test
  public void appendWithKnnInTheSubsearch() {
    verifyLogical(
        knnPlan("source=DOCS | append [ source=VECS | kmeans centroids=1 ]"),
        "LogicalProject(doc_id=[$0], title=[$1], _score=[$3], embedding=[$4])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1], _id=[$2], _score=[null:REAL],"
            + " embedding=[null:VECTOR(3, FLOAT, l2)])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n"
            + "    LogicalProject(doc_id=[$0], title=[null:VARCHAR], _id=[$2], _score=[$3],"
            + " embedding=[$1])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[1])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 1))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /**
   * appendcol, k-NN in the subsearch: the main search's hidden _score is dropped, so _score ($4) is
   * the subsearch's.
   */
  @Test
  public void appendcolWithKnnInTheSubsearch() {
    verifyLogical(
        knnPlan("source=VECS | appendcol [ kmeans centroids=2 ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$4])\n"
            + "  LogicalSort(sort0=[$3], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalJoin(condition=[=($3, $5)], joinType=[full])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _row_number_main_=[ROW_NUMBER() OVER (ORDER BY $0 NULLS LAST)])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(_score=[$3], _row_number_subsearch_=[ROW_NUMBER() OVER (ORDER"
            + " BY $3 DESC NULLS LAST)])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /**
   * appendcol override=true: _score ($7) is the subsearch's k-NN score, with no CASE over the main
   * search's hidden one.
   */
  @Test
  public void appendcolOverrideWithKnnInTheSubsearch() {
    verifyLogical(
        knnPlan("source=VECS | appendcol override=true [ kmeans centroids=2 ]"),
        "LogicalProject(doc_id=[CASE(=($3, $8), $4, $0)], embedding=[CASE(=($3, $8), $5, $1)],"
            + " _score=[$7])\n"
            + "  LogicalSort(sort0=[$3], sort1=[$8], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalJoin(condition=[=($3, $8)], joinType=[full])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _row_number_main_=[ROW_NUMBER() OVER (ORDER BY $0 NULLS LAST)])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[$3],"
            + " _row_number_subsearch_=[ROW_NUMBER() OVER (ORDER BY $3 DESC NULLS LAST)])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /** appendcol, k-NN in the main search: _score ($3) is the main search's k-NN score. */
  @Test
  public void appendcolWithKnnInTheMainSearch() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | appendcol [ stats count() as c ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], c=[$5])\n"
            + "  LogicalSort(sort0=[$4], sort1=[$6], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalJoin(condition=[=($4, $6)], joinType=[full])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[$3],"
            + " _row_number_main_=[ROW_NUMBER() OVER (ORDER BY $3 DESC NULLS LAST)])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(c=[$0], _row_number_subsearch_=[ROW_NUMBER() OVER ()])\n"
            + "        LogicalAggregate(group=[{}], c=[COUNT()])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /** appendpipe, k-NN only in the sub-pipeline: the main rows get a null _score. */
  @Test
  public void appendpipeWithKnnInTheSubPipeline() {
    verifyLogical(
        knnPlan("source=VECS | appendpipe [ kmeans centroids=2 ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[null:REAL])\n"
            + "      LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /** multisearch: the k-NN branch keeps its _score, the other branch has null. */
  @Test
  public void multisearchGivesNullScoresToTheOtherBranches() {
    verifyLogical(
        knnPlan("| multisearch [ search source=VECS | kmeans centroids=2 ] [ search source=DOCS ]"),
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], title=[null:VARCHAR])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[null:VECTOR(3, FLOAT, l2)],"
            + " _score=[null:REAL], title=[$1])\n"
            + "    LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** union: the k-NN dataset keeps its _score, the other dataset's hidden one becomes null. */
  @Test
  public void unionGivesNullScoresToTheOtherDatasets() {
    verifyLogical(
        knnPlan("| union [ search source=VECS | kmeans centroids=2 ] [ search source=VECS ]"),
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[$1], _score=[null:REAL])\n"
            + "    LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /**
   * A join without keys uses every column both inputs share as a key. Both inputs have a k-NN
   * _score, so it would join rows on equal scores: rejected with a hint.
   */
  @Test
  public void keylessJoinOfTwoKnnInputsIsRejected() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                knnPlan(
                    "source=VECS | kmeans centroids=2 | fields - embedding | join [ source=VECS |"
                        + " kmeans centroids=2 | fields - embedding ]"));
    assertEquals(KnnErrors.keylessJoinOnScore().getMessage(), e.getMessage());
    assertTrue(e.isWithoutCalciteFallback());
  }

  /** Removing _score from one input, or naming the keys, joins as usual. */
  @Test
  public void keylessJoinOfTwoKnnInputsWithoutSharedScore() {
    knnPlan(
        "source=VECS | kmeans centroids=2 | fields - embedding, _score | join [ source=VECS |"
            + " kmeans centroids=2 | fields - embedding ]");
    knnPlan(
        "source=VECS | kmeans centroids=2 | fields - embedding | join doc_id [ source=VECS |"
            + " kmeans centroids=2 | fields - embedding ]");
    // Only one k-NN input: the other input's metadata _score is dropped, so nothing is shared.
    knnPlan("source=VECS | kmeans centroids=2 | fields - embedding | join DOCS");
  }

  /** join, k-NN in the right input: _score ($4) is the right input's k-NN score. */
  @Test
  public void joinWithKnnInTheRightInput() {
    verifyLogical(
        knnPlan(
            "source=DOCS | join left=d right=v on d.doc_id = v.doc_id [ source=VECS | kmeans"
                + " centroids=10 ]"),
        "LogicalProject(doc_id=[$0], title=[$1], v.doc_id=[$2], embedding=[$3], _score=[$4])\n"
            + "  LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 10))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /** graphlookup after a k-NN step: the table's metadata, _score included, is still stripped. */
  @Test
  public void graphlookupTableLosesItsHiddenScoreAfterKnn() {
    verifyLogical(
        knnPlan(
            "source=VECS | kmeans centroids=2 | graphLookup DOCS start=doc_id edge=doc_id-->doc_id"
                + " as h"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], h=[$4])\n"
            + "  LogicalGraphLookup(fromField=[doc_id], toField=[doc_id], outputField=[h],"
            + " depthField=[null], maxDepth=[0], bidirectional=[false])\n"
            + "    LogicalSort(fetch=[100])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** In a k-NN branch _score is a regular column for wildcard renames, too; _id is not. */
  @Test
  public void wildcardRenameIncludesTheKnnScore() {
    assertEquals(
        List.of("x_doc_id", "x_embedding", "x__score"),
        knnPlan("source=VECS | kmeans centroids=2 | rename * as x_*").getRowType().getFieldNames());
    assertEquals(
        List.of("x_doc_id", "x_embedding"),
        getRelNode("source=VECS | rename * as x_*").getRowType().getFieldNames());
  }

  /**
   * After {@code fields - _score} there is no k-NN score to keep: DOCS's hidden _score is dropped,
   * not nulled.
   */
  @Test
  public void appendWithoutAKnnScoreLeftHasNoScore() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | fields - _score | append [ source=DOCS ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1], title=[$3])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], title=[null:VARCHAR])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[null:VECTOR(3, FLOAT, l2)], _id=[$2],"
            + " title=[$1])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** The same for appendpipe: the main search's hidden _score is dropped. */
  @Test
  public void appendpipeWithoutAKnnScoreLeftHasNoScore() {
    verifyLogical(
        knnPlan("source=VECS | appendpipe [ kmeans centroids=2 | fields - _score ]"),
        "LogicalProject(doc_id=[$0], embedding=[$1])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2])\n"
            + "      LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2])\n"
            + "      LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "        LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /**
   * appendcol override=true with k-NN in the main search: _score ($3) stays the k-NN score instead
   * of a CASE over the subsearch's hidden one.
   */
  @Test
  public void appendcolOverrideWithKnnInTheMainSearch() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | appendcol override=true [ where doc_id > 0 ]"),
        "LogicalProject(doc_id=[CASE(=($4, $8), $5, $0)], embedding=[CASE(=($4, $8), $6, $1)],"
            + " _score=[$3])\n"
            + "  LogicalSort(sort0=[$4], sort1=[$8], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalJoin(condition=[=($4, $8)], joinType=[full])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[$3],"
            + " _row_number_main_=[ROW_NUMBER() OVER (ORDER BY $3 DESC NULLS LAST)])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _row_number_subsearch_=[ROW_NUMBER() OVER (ORDER BY $0 NULLS LAST)])\n"
            + "        LogicalFilter(condition=[>($0, 0)])\n"
            + "          LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /** A lookup cannot take _score from the lookup table after a k-NN step. */
  @Test
  public void lookupCannotOutputTheTableScoreAfterKnn() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | kmeans centroids=2 | lookup DOCS doc_id output _score"));
    assertEquals(KnnErrors.lookupTableScore().getMessage(), e.getMessage());
  }

  /** A k-NN step in an appendpipe sub-pipeline is a second one in the main search's branch. */
  @Test
  public void knnInTheMainSearchAndInAnAppendpipeIsMoreThanOne() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | kmeans | appendpipe [ kmeans centroids=3 ]"));
    assertEquals(KnnErrors.MORE_THAN_ONE_KNN_MESSAGE, e.getMessage());
  }

  /**
   * Lowered twice (by knnPlan), as the profiling path does: the second pass must not reach the main
   * k-NN step through the sub-pipeline it was attached to and take the sub-pipeline's head.
   */
  @Test
  public void appendpipeAfterKnnLowersTheSameTwice() {
    List<String> names =
        knnPlan("source=VECS | kmeans centroids=10 | appendpipe [ head 3 from 8 ]")
            .getRowType()
            .getFieldNames();
    assertEquals(List.of("doc_id", "embedding", "_score"), names);
  }

  /** A k-NN step in an appendpipe after head is misplaced; that, not k, is the error. */
  @Test
  public void knnInAnAppendpipeAfterHeadIsAPlacementError() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | head 5 | appendpipe [ kmeans ]"));
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, e.getMessage());
  }

  /** No lookup output may be named _score after a k-NN step: it would replace the k-NN score. */
  @Test
  public void lookupCannotOutputAFieldAsScoreAfterKnn() {
    for (String ppl :
        List.of(
            "source=VECS | kmeans centroids=2 | lookup DOCS doc_id output title as _score",
            "source=VECS | kmeans centroids=2 | lookup DOCS doc_id replace title as _score",
            "source=VECS | kmeans centroids=2 | lookup DOCS doc_id append title as _score")) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> knnPlan(ppl));
      assertEquals(ppl, KnnErrors.scoreReplaced("lookup").getMessage(), e.getMessage());
    }
  }

  /** The k-NN _score may still be the source side of a lookup key. */
  @Test
  public void lookupMayMatchOnTheKnnScore() {
    verifyLogical(
        knnPlan("source=VECS | kmeans centroids=2 | lookup DOCS doc_id as _score"),
        "LogicalProject(doc_id=[$0], embedding=[$1], _score=[$3], title=[$5])\n"
            + "  LogicalJoin(condition=[=($3, $4)], joinType=[left])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "        LogicalTableScan(table=[[scott, VECS]])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1], _id=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  /** eval and rename cannot write _score at all, so they cannot overwrite the k-NN score. */
  @Test
  public void evalAndRenameCannotOverwriteTheKnnScore() {
    IllegalArgumentException rename =
        assertThrows(
            IllegalArgumentException.class,
            () -> knnPlan("source=VECS | kmeans centroids=2 | rename doc_id as _score"));
    assertEquals("Cannot use metadata field [_score] in Rename command.", rename.getMessage());
    IllegalArgumentException eval =
        assertThrows(
            IllegalArgumentException.class,
            () -> knnPlan("source=VECS | kmeans centroids=2 | eval _score = 1"));
    assertEquals("Cannot use metadata field [_score] as the eval field.", eval.getMessage());
  }

  /**
   * After a k-NN step, no command may leave a _score column that is not the k-NN score. Each
   * command that can name an output column is tried: the parser already refuses _score for eval,
   * rename, stats, chart and the like, top/rare refuse an existing count field name, and the
   * planner refuses the rest, naming the command. Every error is a client error (HTTP 400).
   */
  @Test
  public void noCommandCanReplaceTheKnnScore() {
    doReturn(10).when(settings).getSettingValue(Key.PPL_REX_MAX_MATCH_LIMIT);
    doReturn("simple_pattern").when(settings).getSettingValue(Key.PATTERN_METHOD);
    doReturn("label").when(settings).getSettingValue(Key.PATTERN_MODE);
    doReturn(10).when(settings).getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT);
    doReturn(100000).when(settings).getSettingValue(Key.PATTERN_BUFFER_LIMIT);
    doReturn(false).when(settings).getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN);
    String knn = "source=VECS | kmeans centroids=2 | ";
    String metadataRename = "Cannot use metadata field [_score] in Rename command.";
    String metadataEval = "Cannot use metadata field [_score] as the eval field.";
    String metadataAlias = "Cannot use metadata field [_score] as the alias.";
    String countField =
        "Field `_score` is existed, change the count field by setting countfield='xyz'";
    String[][] cases = {
      {"rename doc_id as _score", metadataRename},
      {"rename *mbedding as _scor*", replaced("rename")},
      {"eval _score = 1", metadataEval},
      {"trendline sma(2, doc_id) as _score", replaced("trendline")},
      {"bin doc_id span=1 as _score", replaced("bin")},
      {"eventstats count() as _score", metadataAlias},
      {"streamstats count() as _score", metadataAlias},
      {"stats count() as _score", metadataAlias},
      {"top 1 countfield='_score' doc_id", countField},
      {"rare countfield='_score' doc_id", countField},
      {"chart count() as _score by doc_id", metadataAlias},
      {"timechart timefield=doc_id span=1 count() as _score", metadataAlias},
      {"fillnull with 0 in _score", replaced("fillnull")},
      {"convert num(doc_id) as _score", metadataEval},
      {"eval s = '{}' | spath input=s output=_score path=a", metadataEval},
      {"eval s = 'abc' | rex field=s '(?<_score>.*)'", "Invalid capture group name '_score'."},
      {"eval s = 'abc' | parse s '(?<_score>.*)'", "Invalid capture group name '_score'."},
      {"eval s = 'abc' | patterns s new_field='_score'", replaced("patterns")},
      {"lookup DOCS doc_id output title as _score", replaced("lookup")},
      {"lookup DOCS doc_id output _score", KnnErrors.lookupTableScore().getMessage()},
      {"lookup DOCS _score", KnnErrors.lookupTableScore().getMessage()},
      {"transpose column_name='_score'", replaced("transpose")},
      {"fields doc_id | transpose column_name='_score'", replaced("transpose")},
      {"eval s = 'a' | xyseries doc_id s in ('_score') doc_id", replaced("xyseries")},
      {"replace 'a' with 'b' in _score", replaced("replace")},
      {"fields doc_id, _score | mvcombine _score", replaced("mvcombine")},
      {"bin _score span=0.1", replaced("bin")},
      {"addtotals col=true _score", replaced("addtotals")},
      {"addcoltotals _score", replaced("addcoltotals")},
      {"graphLookup DOCS start=doc_id edge=doc_id-->doc_id as _score", replaced("graphlookup")},
      {
        "graphLookup DOCS start=_score edge=doc_id-->doc_id batchMode=true as g",
        replaced("graphlookup")
      },
      {"eval s = 'a b' | grok s '%{WORD:_score}'", replaced("grok")},
      {"foreach _score [ eval <<FIELD>> = <<FIELD>> + 1 ]", replaced("foreach")},
      // These would add a second _score, which Calcite renames _score0.
      {"eval a = array(1, 2) | expand a as _score", renamed("expand")},
      {"addtotals fieldname='_score'", renamed("addtotals")},
    };
    for (String[] c : cases) {
      String query = knn + c[0];
      RuntimeException e = assertThrows(query, RuntimeException.class, () -> knnPlan(query));
      Throwable cause = e instanceof ErrorReport report ? report.getCause() : e;
      assertTrue(
          query + " -> " + cause,
          cause instanceof SemanticCheckException || cause instanceof IllegalArgumentException);
      assertEquals(query, c[1], cause.getMessage());
    }
  }

  private static String replaced(String command) {
    return KnnErrors.scoreReplaced(command).getMessage();
  }

  private static String renamed(String command) {
    return KnnErrors.scoreRenamed(command, "_score0").getMessage();
  }

  /**
   * Commands that only pass the k-NN score through keep working: a join with a second index (whose
   * hidden _score is dropped), addcoltotals of listed fields, and addtotals, which adds a new
   * column.
   */
  @Test
  public void commandsThatPassTheKnnScoreThroughKeepWorking() {
    String knn = "source=VECS | kmeans centroids=2 | ";
    for (String rest :
        List.of(
            "join left=a right=b on a.doc_id = b.doc_id DOCS",
            "addcoltotals doc_id",
            "addtotals fieldname='total'",
            "addcoltotals",
            "fields - embedding | addcoltotals",
            "fields - embedding | addtotals col=true",
            "fields doc_id, _score | fillnull with 0",
            "eval s = _score * 2 | sort - _score | fields doc_id, _score, s",
            "rename _score as similarity")) {
      knnPlan(knn + rest);
    }
  }

  /**
   * A k-NN step without k in a subsearch with an empty source is pruned before lowering; its
   * missing k is still reported.
   */
  @Test
  public void aPrunedKnnStepStillReportsItsMissingK() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class, () -> knnPlan("source=VECS | append [ | kmeans ]"));
    assertTrue(e.getMessage(), e.getMessage().startsWith(KnnErrors.MISSING_K_MESSAGE));
  }

  /**
   * Error order across branches is fixed: argument errors (here k=0 in the subsearch) come from the
   * AST pass, before the lowering raises errors about k (the main search's missing k).
   */
  @Test
  public void argumentErrorsOfAnyBranchComeBeforeErrorsAboutK() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | kmeans | append [ source=VECS | kmeans centroids=0 ]"));
    assertEquals("k must be between 1 and 10000, got [0]", e.getMessage());
  }

  /**
   * Commands over all fields leave the k-NN score out: the totals row of addcoltotals / addtotals
   * col=true has a null _score, the row total of addtotals does not add _score in, and fillnull
   * without a field list leaves _score alone. Naming _score explicitly stays refused.
   */
  @Test
  public void commandsOverAllFieldsLeaveTheKnnScoreOut() {
    String knn = "source=VECS | kmeans centroids=2 | ";
    verifyLogical(
        knnPlan(knn + "fields - embedding | addcoltotals"),
        "LogicalProject(doc_id=[$0], _score=[$2])\n"
            + "  LogicalSort(sort0=[$3], dir0=[ASC])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalProject(doc_id=[CAST($0):BIGINT NOT NULL], _id=[$2], _score=[$3],"
            + " _addcoltotals_order_=[0])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(doc_id=[$0], _id=[null:VARCHAR], _score=[null:REAL],"
            + " _addcoltotals_order_=[1])\n"
            + "        LogicalAggregate(group=[{}], doc_id=[SUM($0)])\n"
            + "          LogicalProject(doc_id0=[CAST($0):BIGINT NOT NULL])\n"
            + "            LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "              LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "                LogicalTableScan(table=[[scott, VECS]])\n");
    // The row total leaves _score out too.
    verifyLogical(
        knnPlan(knn + "fields - embedding | addtotals col=true"),
        "LogicalProject(doc_id=[$0], _score=[$2], Total=[$3])\n"
            + "  LogicalSort(sort0=[$4], dir0=[ASC])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalProject(doc_id=[CAST($0):BIGINT NOT NULL], _id=[$2], _score=[$3],"
            + " Total=[CAST($0):BIGINT NOT NULL], _addcoltotals_order_=[0])\n"
            + "        LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "          LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "            LogicalTableScan(table=[[scott, VECS]])\n"
            + "      LogicalProject(doc_id=[$0], _id=[null:VARCHAR], _score=[null:REAL],"
            + " Total=[null:BIGINT], _addcoltotals_order_=[1])\n"
            + "        LogicalAggregate(group=[{}], doc_id=[SUM($0)])\n"
            + "          LogicalProject(doc_id0=[CAST($0):BIGINT NOT NULL])\n"
            + "            LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "              LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2],"
            + " _score=[KNN_SCORE($1, ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "                LogicalTableScan(table=[[scott, VECS]])\n");
    verifyLogical(
        knnPlan(knn + "fields doc_id, _score | fillnull with 0"),
        "LogicalProject(doc_id=[COALESCE($0, 0)], _score=[$3])\n"
            + "  LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 2))])\n"
            + "      LogicalTableScan(table=[[scott, VECS]])\n");
  }

  /**
   * A join input reduced to the scan's own _score would have no column left once that is dropped:
   * the error says so rather than blaming a command after knn.
   */
  @Test
  public void aJoinInputWithOnlyTheScanScoreCannotBeJoinedWithAKnnInput() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                knnPlan(
                    "source=VECS | kmeans centroids=2 | join left=a right=b on a._score = b._score"
                        + " [ source=DOCS | fields _score ]"));
    assertEquals(KnnErrors.onlyMetadataScore("join").getMessage(), e.getMessage());
  }

  /**
   * appendcol drops the scan's _score of the input without knn after adding its row number, so an
   * input of only that column still works: it adds no column, and _score is the k-NN score.
   */
  @Test
  public void appendcolTakesAnInputWithOnlyTheScanScore() {
    for (String ppl :
        List.of(
            "source=VECS | kmeans centroids=2 | appendcol [ fields _score ]",
            "source=VECS | kmeans centroids=2 | appendcol override=true [ fields _score ]",
            "source=VECS | fields _score | appendcol [ kmeans centroids=2 ]")) {
      RelNode plan = knnPlan(ppl);
      List<String> names = plan.getRowType().getFieldNames();
      // The columns that matter only: the hidden _id shows here through an upstream defect of
      // the projection bookkeeping (isProjectVisited), fixed separately and not on this branch.
      assertTrue(ppl, names.containsAll(List.of("doc_id", "embedding")));
      assertEquals(ppl, 1, count(names, "_score"));
      assertTrue(ppl, KnnPlanBuilder.isKnnScoreColumn(plan, names.indexOf("_score")));
      assertTrue(ppl, names.stream().noneMatch(name -> name.matches("_score[0-9]+")));
    }
  }

  /**
   * A table field named _score0 is a column like any other: passed through unchanged, it is not a
   * second _score. A _score0 a command adds is refused even when such a field is scanned elsewhere
   * in the plan, or was there before and has been removed.
   */
  @Test
  public void aScanColumnNamedLikeASecondScoreIsKeptButNoCommandMayAddOne() {
    for (String ppl :
        List.of(
            "source=S0 | kmeans centroids=2",
            "source=S0 | kmeans centroids=2 | eval a = array(1, 2) | expand a",
            "source=VECS | kmeans centroids=2 | append [ source=S0 ]",
            "source=VECS | kmeans centroids=2 | join left=a right=b on a.doc_id = b.doc_id ["
                + " source=S0 | fields doc_id, _score0 ]")) {
      assertTrue(ppl, knnPlan(ppl).getRowType().getFieldNames().contains("_score0"));
    }
    String[][] cases = {
      {
        "source=VECS | kmeans centroids=2 | join left=a right=b on a.doc_id = b.doc_id [ source=S0"
            + " | fields doc_id ] | addtotals fieldname='_score'",
        "addtotals"
      },
      {
        "source=S0 | kmeans centroids=2 | fields - _score0 | eval a = array(1, 2) | expand a as"
            + " _score",
        "expand"
      },
    };
    for (String[] c : cases) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> knnPlan(c[0]));
      assertEquals(c[0], renamed(c[1]), e.getMessage());
    }
  }

  /**
   * An input without knn combined with a k-NN input may not bring a _score of its own: combined,
   * _score is the k-NN score, so its values would be dropped or nulled. The error names the
   * combining command. The scan's own _score of such an input is dropped or nulled as before.
   */
  @Test
  public void anInputWithoutKnnCannotBringAScoreOfItsOwn() {
    String knn = "source=VECS | kmeans centroids=2";
    String own = "source=DOCS | fields - _score | eval a = array(1.5, 2.5) | expand a as _score";
    String[][] cases = {
      {knn + " | join left=a right=b on a.doc_id = b.doc_id [ " + own + " ]", "join"},
      {
        knn
            + " | join left=a right=b on a.doc_id = b.doc_id [ source=DOCS | rename titl* as _scor*"
            + " ]",
        "join"
      },
      {
        knn + " | appendcol [ fields - _score | eval a = array(1.5, 2.5) | expand a as _score ]",
        "appendcol"
      },
      {knn + " | append [ " + own + " ]", "append"},
      {knn + " | union [ " + own + " ]", "union"},
      {"| multisearch [ " + knn + " ] [ " + own + " ]", "multisearch"},
      {"source=VECS | trendline sma(2, doc_id) as _score | append [ " + knn + " ]", "append"},
      {knn + " | append [ source=VECS | bin doc_id span=1 as _score ]", "append"},
    };
    for (String[] c : cases) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> knnPlan(c[0]));
      assertEquals(c[0], KnnErrors.inputScore(c[1]).getMessage(), e.getMessage());
    }
    // The scan's _score, and commands over all fields, which leave it alone, are fine.
    for (String ppl :
        List.of(
            knn
                + " | join left=a right=b on a.doc_id = b.doc_id [ source=DOCS | where _score > 0"
                + " ]",
            knn + " | union [ source=DOCS | sort _score ]",
            knn + " | fields _score | append [ source=VECS | fields doc_id | fillnull with 0 ]",
            knn + " | fields _score | append [ source=VECS | fields - embedding | addcoltotals ]",
            knn + " | fields _score | append [ source=VECS | addtotals col=true ]")) {
      RelNode plan = knnPlan(ppl);
      List<String> names = plan.getRowType().getFieldNames();
      assertTrue(ppl, KnnPlanBuilder.isKnnScoreColumn(plan, names.indexOf("_score")));
    }
  }

  /**
   * A join with a k-NN input keeps the other input's table alias on the columns that input
   * computes: the scan's _score of that input is dropped inside it, before the alias applies, so
   * that no projection after the alias loses it.
   */
  @Test
  public void aJoinWithAKnnInputKeepsTheOtherInputsAliasOnComputedColumns() {
    String knn = "source=VECS | kmeans centroids=5";
    String join = " | join left=a right=b on a.doc_id = b.doc_id ";
    String docsX = "[ source=DOCS | eval x = 1 ]";
    for (String ppl :
        List.of(
            knn + join + docsX + " | where b.x = 1",
            knn + join + docsX + " | fields a.doc_id, b.x, _score",
            knn + join + docsX + " | stats count() by b.x",
            knn
                + " | join type=left left=a right=b on a.doc_id = b.doc_id "
                + docsX
                + " | where b.x = 1",
            knn + join + "[ source=DOCS | eval doc_id = doc_id + 1 ]",
            knn + join + "[ source=DOCS | bin doc_id span=1 ]",
            knn + join + "[ source=DOCS | fillnull with 0 in doc_id ]",
            knn + join + "[ source=DOCS | convert num(doc_id) ]",
            "source=DOCS | eval x = 1" + join + "[ " + knn + " ] | where a.x = 1")) {
      RelNode plan = knnPlan(ppl);
      List<String> names = plan.getRowType().getFieldNames();
      int score = names.indexOf("_score");
      assertTrue(ppl, score < 0 || KnnPlanBuilder.isKnnScoreColumn(plan, score));
      assertTrue(ppl, names.stream().noneMatch(name -> name.matches("_score[0-9]+")));
    }
  }

  /**
   * appendcol in an append subsearch fails, also without knn: the empty-source pass leaves it
   * without a subsearch (an upstream defect). The k-NN code on the way, which looks at that
   * subsearch, must not be where it fails, with or without a k-NN step in the query.
   */
  @Test
  public void appendcolInAnAppendDoesNotFailInTheKnnCode() {
    String inner = " | append [ source=VECS | appendcol [ stats count() as c ] ]";
    for (String ppl : List.of("source=VECS" + inner, "source=VECS | kmeans centroids=2" + inner)) {
      try {
        knnPlan(ppl);
      } catch (RuntimeException e) {
        Throwable cause = e instanceof ErrorReport report ? report.getCause() : e;
        StackTraceElement top = cause.getStackTrace()[0];
        assertTrue(
            ppl + " -> " + top, !top.getClassName().startsWith("org.opensearch.sql.calcite.knn."));
      }
    }
  }

  /**
   * A command that adds a column named _score next to the k-NN score gets it renamed _score0 by
   * Calcite; whichever command it is, the check after every command refuses it. A column the query
   * itself names _score0 is refused too: it cannot be told apart.
   */
  @Test
  public void noCommandAddsASecondScoreNextToTheKnnScore() {
    String[][] cases = {
      {"source=OBJS | kmeans centroids=2 | flatten obj as (_score, b)", "flatten"},
      // A struct sub-field named _score, flattened under its own name.
      {"source=OBJS | kmeans centroids=2 | flatten obj", "flatten"},
      {"source=VECS | kmeans centroids=2 | eval a = array(1, 2) | expand a as _score", "expand"},
      {"source=VECS | kmeans centroids=2 | addtotals fieldname='_score'", "addtotals"},
      {"source=VECS | kmeans centroids=2 | eval _score0 = 1", "eval"},
    };
    for (String[] c : cases) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> knnPlan(c[0]));
      assertEquals(c[0], renamed(c[1]), e.getMessage());
    }
    // Under other names the same commands work.
    knnPlan("source=OBJS | kmeans centroids=2 | flatten obj as (a, b)");
  }

  /**
   * The same holds for the rows of every input combined with a k-NN input, and for the main search
   * before the k-NN input is combined: the result has the k-NN _score, so a _score0 next to it
   * would look like another score.
   */
  @Test
  public void noInputCombinedWithAKnnInputAddsASecondScore() {
    String knn = "source=VECS | kmeans centroids=2";
    String expand = "eval a = array(1, 2) | expand a as _score";
    String totals = "addtotals fieldname='_score'";
    String[][] cases = {
      {knn + " | appendcol [ " + expand + " ]", "expand"},
      {knn + " | append [ source=DOCS | " + totals + " ]", "addtotals"},
      {knn + " | fields - _score | append [ source=DOCS | " + totals + " ]", "addtotals"},
      {knn + " | appendpipe [ " + expand + " ]", "expand"},
      {
        knn + " | join left=a right=b on a.doc_id = b.doc_id [ source=DOCS | " + totals + " ]",
        "addtotals"
      },
      {"| multisearch [ " + knn + " ] [ source=DOCS | " + totals + " ]", "addtotals"},
      {"| union [ " + knn + " ] [ source=DOCS | " + totals + " ]", "addtotals"},
      {knn + " | union [ source=DOCS | " + totals + " ]", "addtotals"},
      {"source=DOCS | " + totals + " | append [ " + knn + " ]", "addtotals"},
    };
    for (String[] c : cases) {
      SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> knnPlan(c[0]));
      assertEquals(c[0], renamed(c[1]), e.getMessage());
    }
  }

  /**
   * Without a k-NN score in the result, a second _score is renamed _score0 as before, also when a
   * subsearch in an expression has a k-NN step.
   */
  @Test
  public void aSecondScoreIsRenamedAsBeforeInAResultWithoutKnnScore() {
    for (String ppl :
        List.of(
            "source=DOCS | eval a = array(1, 2) | expand a as _score",
            "source=DOCS | eval a = array(1, 2) | expand a as _score | where doc_id in ["
                + " source=VECS | kmeans centroids=2 | fields doc_id ]")) {
      assertEquals(
          ppl, List.of("doc_id", "title", "_score0"), knnPlan(ppl).getRowType().getFieldNames());
    }
  }

  /**
   * A graphlookup passes the k-NN _score of its source through in its normal mode; its depth field
   * is a field of the array elements, so it may be named _score. In batch mode every column is an
   * array, so a start field named _score is not the k-NN score.
   */
  @Test
  public void graphLookupPassesTheKnnScoreThroughInItsNormalMode() {
    String knn = "source=VECS | kmeans centroids=2 | ";
    RelNode plan =
        knnPlan(knn + "graphLookup DOCS start=doc_id edge=doc_id-->doc_id depthField=_score as d");
    List<String> names = plan.getRowType().getFieldNames();
    assertEquals(List.of("doc_id", "embedding", "_score", "d"), names);
    assertTrue(KnnPlanBuilder.isKnnScoreColumn(plan, names.indexOf("_score")));
    SemanticCheckException batch =
        assertThrows(
            SemanticCheckException.class,
            () ->
                knnPlan(
                    knn
                        + "graphLookup DOCS start=_score edge=doc_id-->doc_id batchMode=true as"
                        + " g"));
    assertEquals(replaced("graphlookup"), batch.getMessage());
  }

  /**
   * foreach over a wildcard leaves the k-NN score out, as fillnull over all fields does; naming
   * _score is refused.
   */
  @Test
  public void foreachOverAWildcardLeavesTheKnnScoreOut() {
    RelNode plan =
        knnPlan(
            "source=VECS | kmeans centroids=2 | fields doc_id, _score | foreach * [ eval"
                + " <<FIELD>> = <<FIELD>> + 1 ]");
    List<String> names = plan.getRowType().getFieldNames();
    assertEquals(List.of("_score", "doc_id"), names);
    assertTrue(KnnPlanBuilder.isKnnScoreColumn(plan, names.indexOf("_score")));
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                knnPlan(
                    "source=VECS | kmeans centroids=2 | foreach _score [ eval <<FIELD>> ="
                        + " <<FIELD>> + 1 ]"));
    assertEquals(replaced("foreach"), e.getMessage());
  }

  @Test
  public void knnInAnInSubsearch() {
    verifyLogical(
        knnPlan(
            "source=DOCS | where doc_id in [ source=VECS | kmeans centroids=1 | fields doc_id ]"
                + " | fields title"),
        "LogicalProject(title=[$1])\n"
            + "  LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(doc_id=[$0])\n"
            + "  LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[1])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(0.5E0:REAL, 1.0E0:REAL, 1.5E0:REAL), MAP('k', 1))])\n"
            + "      LogicalTableScan(table=[[scott, VECS]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, DOCS]])\n");
  }

  @Test
  public void oneKnnPerJoinInput() {
    List<String> names =
        knnPlan(
                "source=VECS | kmeans centroids=3 | join left=a right=b on a.doc_id = b.doc_id ["
                    + " source=VECS | kmeans centroids=2 ]")
            .getRowType()
            .getFieldNames();
    assertEquals(names.toString(), 2, names.stream().filter(n -> n.endsWith("_score")).count());
  }

  @Test
  public void twoKnnInOneBranchFail() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> knnPlan("source=VECS | kmeans centroids=3 | kmeans centroids=2"));
    assertEquals(KnnErrors.MORE_THAN_ONE_KNN_MESSAGE, e.getMessage());
  }

  /** Without a pushdown, executing KNN_SCORE fails; Calcite keeps the error as suppressed. */
  @Test
  public void executingAnUnpushedKnnScoreFails() {
    RelNode root = knnPlan("source=VECS | kmeans centroids=2");
    Exception e =
        assertThrows(
            Exception.class,
            () -> {
              try (PreparedStatement statement = RelRunners.run(root)) {
                statement.executeQuery();
              }
            });
    verifyErrorMessageContains(e, KnnErrors.NOT_PUSHED_DOWN_MESSAGE);
  }

  /** Where the property test runs a query, with the query around the commands under test. */
  private enum Place {
    AFTER_KNN("after knn", "%1$s | %2$s"),
    APPEND_INPUT("append input", "%3$s | append [ source=%4$s | %2$s ]"),
    APPEND_BEFORE("before append", "source=%4$s | %2$s | append [ %3$s ]"),
    JOIN_INPUT("join input", "%3$s | join left=a right=b on 1 = 1 [ source=%4$s | %2$s ]"),
    JOIN_BEFORE("before join", "source=%4$s | %2$s | join left=a right=b on 1 = 1 [ %3$s ]"),
    MULTISEARCH_INPUT("multisearch input", "| multisearch [ %3$s ] [ source=%4$s | %2$s ]"),
    MULTISEARCH_BEFORE("before multisearch", "| multisearch [ source=%4$s | %2$s ] [ %3$s ]"),
    UNION_INPUT("union input", "%3$s | union [ source=%4$s | %2$s ]"),
    UNION_BEFORE("before union", "source=%4$s | %2$s | union [ %3$s ]"),
    APPENDCOL_INPUT("appendcol input", "%3$s | appendcol [ %2$s ]"),
    APPENDCOL_BEFORE(
        "before appendcol",
        "source=%4$s | %2$s | appendcol [ kmeans centroids=2 | fields _score ]");

    private final String label;
    private final String template;

    Place(String label, String template) {
      this.label = label;
      this.template = template;
    }

    /**
     * The query: {@code rest} after a k-NN step on {@code table}, or in an input without knn that
     * is combined with a k-NN input of only {@code _score}, so that no other column is shared and
     * the combining command's own checks on shared columns are not what decides.
     */
    String query(String rest, String table) {
      String knn = "source=" + table + " | kmeans centroids=2";
      return String.format(
          template, knn, rest, this == AFTER_KNN ? knn : knn + " | fields _score", table);
    }

    /** The combining command, which errors about an input name. */
    String command() {
      return name().substring(0, name().indexOf('_')).toLowerCase(java.util.Locale.ROOT);
    }
  }

  /** The expected outcome of a query in one place. */
  private record Outcome(Kind kind, @Nullable String message) {
    enum Kind {
      /** A plan whose _score columns are all the k-NN score and with no _score0-like column. */
      PLAN,
      /** A k-NN error, marked never to fall back, with this message. */
      KNN_ERROR,
      /** A client error (HTTP 400) of a check that is not a k-NN one, with this message. */
      REFUSAL,
      /** Not run, for a defect that is not a k-NN one, named in the message. */
      SKIP
    }
  }

  private static final Outcome PLAN = new Outcome(Outcome.Kind.PLAN, null);

  /** In a k-NN error message, the combining command of the place. */
  private static final String COMBINER = "{combiner}";

  private static Outcome knnError(String message) {
    return new Outcome(Outcome.Kind.KNN_ERROR, message);
  }

  private static Outcome refusal(String message) {
    return new Outcome(Outcome.Kind.REFUSAL, message);
  }

  private static Outcome skip(String defect) {
    return new Outcome(Outcome.Kind.SKIP, defect);
  }

  /**
   * A query of the property test: the commands under test and their expected outcome after knn and
   * in the other places, with exceptions for single places.
   */
  private record Case(String rest, Outcome afterKnn, Outcome combined, Map<Place, Outcome> except) {
    Case(String rest, Outcome afterKnn, Outcome combined) {
      this(rest, afterKnn, combined, Map.of());
    }

    Case except(Place place, Outcome outcome) {
      Map<Place, Outcome> map = new java.util.EnumMap<>(Place.class);
      map.putAll(except);
      map.put(place, outcome);
      return new Case(rest, afterKnn, combined, map);
    }

    Outcome expected(Place place) {
      return except.getOrDefault(place, place == Place.AFTER_KNN ? afterKnn : combined);
    }
  }

  /** A query that gives a plan everywhere. */
  private static Case works(String rest) {
    return new Case(rest, PLAN, PLAN);
  }

  /** A query with the same outcome everywhere. */
  private static Case everywhere(String rest, Outcome outcome) {
    return new Case(rest, outcome, outcome);
  }

  /** A query that replaces the k-NN score after knn and makes a _score of its own elsewhere. */
  private static Case replacesTheScore(String rest, String command) {
    return new Case(rest, knnError(replaced(command)), knnError(inputScore()));
  }

  private static String inputScore() {
    return KnnErrors.inputScore(COMBINER).getMessage();
  }

  private static final String METADATA_EVAL =
      "Cannot use metadata field [_score] as the eval field.";
  private static final String METADATA_ALIAS = "Cannot use metadata field [_score] as the alias.";
  private static final String METADATA_RENAME =
      "Cannot use metadata field [_score] in Rename command.";
  private static final String CAPTURE_GROUP = "Invalid capture group name '_score'.";
  private static final String COUNT_FIELD =
      "Field `_score` is existed, change the count field by setting countfield='xyz'";

  /**
   * appendcol in an append subsearch, or in the first dataset of a union, fails with a
   * NullPointerException, without knn too: the empty-source pass leaves it without a subsearch.
   */
  private static final Outcome APPENDCOL_PRUNED =
      skip("appendcol whose subsearch the empty-source pass removes fails with an NPE");

  /**
   * Queries for every command of the grammar's {@code commands} rule, keyed by rule name: ones that
   * pass the k-NN score through and ones that try to replace it, rename it or add a second one,
   * each with its expected outcome in every place.
   */
  private static final Map<String, List<Case>> COMMAND_CASES =
      Map.ofEntries(
          Map.entry(
              "whereCommand", List.of(works("where doc_id > 1"), works("where _score > 0.5"))),
          Map.entry(
              "fieldsCommand",
              List.of(works("fields doc_id, _score"), works("fields - _score"), works("fields *"))),
          Map.entry("tableCommand", List.of(works("table doc_id, _score"))),
          Map.entry(
              "joinCommand",
              List.of(
                  works("join left=c right=d on c.doc_id = d.doc_id DOCS"),
                  works(
                      "join left=c right=d on c.doc_id = d.doc_id [ source=VECS | kmeans"
                          + " centroids=1 ]"),
                  everywhere(
                      "join left=c right=d on c.doc_id = d.doc_id [ source=DOCS | eval a = array(1,"
                          + " 2) | expand a as _score ]",
                      knnError(renamed("expand"))),
                  // Elsewhere the join is not one with a k-NN input: it strips the column, as
                  // before.
                  new Case(
                      "join left=c right=d on c.doc_id = d.doc_id [ source=DOCS | rename titl* as"
                          + " _scor* ]",
                      knnError(KnnErrors.inputScore("join").getMessage()),
                      PLAN))),
          Map.entry(
              "renameCommand",
              List.of(
                  works("rename doc_id as d"),
                  works("rename _score as s"),
                  everywhere("rename doc_id as _score", refusal(METADATA_RENAME)),
                  replacesTheScore("rename *mbedding as _scor*", "rename"))),
          Map.entry(
              "statsCommand",
              List.of(
                  works("stats avg(_score) as a by doc_id"),
                  everywhere("stats count() as _score", refusal(METADATA_ALIAS)))),
          Map.entry(
              "eventstatsCommand",
              List.of(
                  works("eventstats max(_score) as m"),
                  everywhere("eventstats count() as _score", refusal(METADATA_ALIAS)))),
          Map.entry(
              "streamstatsCommand",
              List.of(
                  works("streamstats count() as c"),
                  everywhere("streamstats count() as _score", refusal(METADATA_ALIAS)))),
          Map.entry("dedupCommand", List.of(works("dedup doc_id"), works("dedup _score"))),
          Map.entry("sortCommand", List.of(works("sort doc_id"), works("sort - _score"))),
          Map.entry(
              "evalCommand",
              List.of(
                  works("eval s = _score * 2"),
                  everywhere("eval _score = 1", refusal(METADATA_EVAL)),
                  everywhere("eval _score0 = 1", knnError(renamed("eval"))))),
          Map.entry(
              "foreachCommand",
              List.of(
                  works("fields doc_id, _score | foreach * [ eval <<FIELD>> = <<FIELD>> + 1 ]"),
                  replacesTheScore(
                      "foreach _score [ eval <<FIELD>> = <<FIELD>> + 1 ]", "foreach"))),
          Map.entry("headCommand", List.of(works("head 1"))),
          Map.entry(
              "binCommand",
              List.of(
                  works("bin doc_id span=1"),
                  replacesTheScore("bin _score span=0.1", "bin"),
                  replacesTheScore("bin doc_id span=1 as _score", "bin"))),
          Map.entry(
              "rareTopCommand",
              List.of(
                  works("top doc_id"),
                  works("rare doc_id"),
                  works("top _score"),
                  everywhere("top 1 countfield='_score' doc_id", refusal(COUNT_FIELD)))),
          Map.entry(
              "grokCommand",
              List.of(
                  works("eval s = 'a b' | grok s '%{WORD:w}'"),
                  replacesTheScore("eval s = 'a b' | grok s '%{WORD:_score}'", "grok"))),
          Map.entry(
              "parseCommand",
              List.of(
                  works("eval s = 'abc' | parse s '(?<p>.*)'"),
                  everywhere("eval s = 'abc' | parse s '(?<_score>.*)'", refusal(CAPTURE_GROUP)))),
          Map.entry(
              "spathCommand",
              List.of(
                  works("eval s = '{}' | spath input=s output=o path=a"),
                  everywhere(
                      "eval s = '{}' | spath input=s output=_score path=a",
                      refusal(METADATA_EVAL)))),
          Map.entry(
              "patternsCommand",
              List.of(
                  works("eval s = 'abc' | patterns s"),
                  replacesTheScore("eval s = 'abc' | patterns s new_field='_score'", "patterns"))),
          Map.entry(
              "lookupCommand",
              List.of(
                  works("lookup DOCS doc_id"),
                  works("lookup DOCS doc_id as _score"),
                  replacesTheScore("lookup DOCS doc_id output title as _score", "lookup"),
                  new Case(
                      "lookup DOCS _score",
                      knnError(KnnErrors.lookupTableScore().getMessage()),
                      PLAN))),
          Map.entry(
              "adCommand",
              List.of(everywhere("ad", refusal("AD command is unsupported in Calcite")))),
          Map.entry(
              "mlCommand",
              List.of(
                  everywhere(
                      "ml action='train' algorithm='rcf'",
                      refusal("ML command is unsupported in Calcite")))),
          Map.entry(
              "fillnullCommand",
              List.of(
                  works("fields doc_id, _score | fillnull with 0"),
                  works("fillnull using doc_id = 0"),
                  replacesTheScore("fillnull with 0 in _score", "fillnull"))),
          Map.entry(
              "convertCommand",
              List.of(
                  works("convert num(doc_id)"),
                  everywhere("convert num(_score)", refusal(METADATA_EVAL)),
                  everywhere("convert num(doc_id) as _score", refusal(METADATA_EVAL)))),
          Map.entry(
              "trendlineCommand",
              List.of(
                  works("trendline sma(2, _score) as t"),
                  works("trendline sma(2, _score)"),
                  replacesTheScore("trendline sma(2, doc_id) as _score", "trendline"))),
          Map.entry(
              "appendcolCommand",
              List.of(
                  works("appendcol [ stats count() as c ]")
                      .except(Place.APPEND_INPUT, APPENDCOL_PRUNED)
                      .except(Place.UNION_BEFORE, APPENDCOL_PRUNED),
                  works("appendcol [ fields _score ]")
                      .except(Place.APPEND_INPUT, APPENDCOL_PRUNED)
                      .except(Place.UNION_BEFORE, APPENDCOL_PRUNED),
                  works("appendcol override=true [ fields _score ]")
                      .except(Place.APPEND_INPUT, APPENDCOL_PRUNED)
                      .except(Place.UNION_BEFORE, APPENDCOL_PRUNED),
                  works("appendcol override=true [ eval s = 1 ]")
                      .except(Place.APPEND_INPUT, APPENDCOL_PRUNED)
                      .except(Place.UNION_BEFORE, APPENDCOL_PRUNED),
                  everywhere(
                          "appendcol [ eval a = array(1, 2) | expand a as _score ]",
                          knnError(renamed("expand")))
                      .except(Place.APPEND_INPUT, APPENDCOL_PRUNED)
                      .except(Place.UNION_BEFORE, APPENDCOL_PRUNED))),
          Map.entry(
              "addtotalsCommand",
              List.of(
                  works("addtotals"),
                  works("addtotals col=true"),
                  replacesTheScore("addtotals col=true _score", "addtotals"),
                  everywhere("addtotals fieldname='_score'", knnError(renamed("addtotals"))),
                  works("addtotals col=true labelfield='_score'"))),
          Map.entry(
              "addcoltotalsCommand",
              List.of(
                  works("addcoltotals"),
                  replacesTheScore("addcoltotals _score", "addcoltotals"),
                  works("addcoltotals labelfield='_score'"))),
          Map.entry(
              "appendCommand",
              List.of(
                  works("append [ source=DOCS ]"),
                  works("append [ source=VECS | kmeans centroids=1 ]"),
                  everywhere(
                      "append [ source=DOCS | addtotals fieldname='_score' ]",
                      knnError(renamed("addtotals"))),
                  everywhere(
                      "fields - _score | append [ source=DOCS | addtotals fieldname='_score' ]",
                      knnError(renamed("addtotals"))))),
          Map.entry(
              "expandCommand",
              List.of(
                  works("eval a = array(1, 2) | expand a"),
                  everywhere(
                      "eval a = array(1, 2) | expand a as _score", knnError(renamed("expand"))))),
          Map.entry(
              "mvexpandCommand",
              List.of(works("mvexpand embedding"), works("eval a = array(1, 2) | mvexpand a"))),
          Map.entry(
              "flattenCommand",
              List.of(
                  works("flatten obj as (a, b)"),
                  everywhere("flatten obj as (_score, b)", knnError(renamed("flatten"))),
                  everywhere("flatten obj", knnError(renamed("flatten"))),
                  everywhere("flatten obj as (b, _score)", knnError(renamed("flatten"))))),
          Map.entry("reverseCommand", List.of(works("reverse"))),
          Map.entry("regexCommand", List.of(works("eval s = 'a' | regex s='a'"))),
          Map.entry(
              "chartCommand",
              List.of(
                  // As the subsearch of appendcol, chart does not find doc_id, without knn too.
                  works("chart avg(_score) by doc_id")
                      .except(Place.APPENDCOL_INPUT, refusal("Field [doc_id] not found.")),
                  everywhere("chart count() as _score by doc_id", refusal(METADATA_ALIAS)))),
          Map.entry(
              "timechartCommand",
              List.of(
                  works("eval t = now() | timechart timefield=t span=1h avg(_score)"),
                  everywhere(
                      "eval t = now() | timechart timefield=t span=1h count() as _score",
                      refusal(METADATA_ALIAS)))),
          Map.entry(
              "transposeCommand",
              List.of(
                  works("fields - embedding | transpose"),
                  new Case(
                      "transpose column_name='_score'",
                      knnError(replaced("transpose")),
                      skip(
                          "transpose column_name='_score' fails with a Calcite assertion in a"
                              + " branch without knn")))),
          Map.entry(
              "rexCommand",
              List.of(
                  works("eval s = 'abc' | rex field=s '(?<r>.*)'"),
                  everywhere(
                      "eval s = 'abc' | rex field=s '(?<_score>.*)'", refusal(CAPTURE_GROUP)))),
          Map.entry(
              "appendPipeCommand",
              List.of(
                  works("appendpipe [ stats count() as c ]"),
                  works("appendpipe [ where doc_id > 1 ]"),
                  everywhere(
                      "appendpipe [ eval a = array(1, 2) | expand a as _score ]",
                      knnError(renamed("expand"))))),
          Map.entry(
              "replaceCommand",
              List.of(
                  works("eval s = 'a' | replace 'a' with 'b' in s"),
                  replacesTheScore("replace 'a' with 'b' in _score", "replace"))),
          Map.entry(
              "mvcombineCommand",
              List.of(
                  works("fields doc_id, _score | mvcombine doc_id"),
                  replacesTheScore("fields doc_id, _score | mvcombine _score", "mvcombine"))),
          Map.entry(
              "fieldformatCommand",
              List.of(
                  works("fieldformat doc_id = doc_id + 1"),
                  everywhere("fieldformat _score = 1", refusal(METADATA_EVAL)))),
          Map.entry(
              "nomvCommand",
              List.of(
                  works("eval a = array(1, 2) | nomv a"),
                  everywhere("nomv _score", refusal(METADATA_EVAL)))),
          Map.entry(
              "graphLookupCommand",
              List.of(
                  works("graphLookup DOCS start=doc_id edge=doc_id-->doc_id as g"),
                  works(
                      "graphLookup DOCS start=doc_id edge=doc_id-->doc_id depthField=_score as d"),
                  new Case(
                      "graphLookup DOCS start=doc_id edge=doc_id-->doc_id as _score",
                      knnError(replaced("graphlookup")),
                      knnError(KnnErrors.scoreRenamed("graphlookup", "_score").getMessage())),
                  replacesTheScore(
                      "graphLookup DOCS start=_score edge=doc_id-->doc_id batchMode=true as g",
                      "graphlookup"))),
          Map.entry(
              "xyseriesCommand",
              List.of(
                  works("eval s = 'a' | xyseries doc_id s in ('a') doc_id"),
                  replacesTheScore(
                      "eval s = 'a' | xyseries doc_id s in ('_score') doc_id", "xyseries"))),
          Map.entry(
              "unionCommand",
              List.of(
                  works("union [ source=DOCS ]")
                      // As the subsearch of appendcol, it has a single dataset ...
                      .except(
                          Place.APPENDCOL_INPUT,
                          refusal("Union command requires at least two datasets. Provided: 1"))
                      // ... and as its main search, the subsearch's k-NN step is not on VECS.
                      .except(
                          Place.APPENDCOL_BEFORE,
                          knnError("knn field [embedding] is not a knn_vector field")),
                  everywhere(
                      "union [ source=DOCS | addtotals fieldname='_score' ]",
                      knnError(renamed("addtotals"))))),
          Map.entry(
              "timewrapCommand",
              List.of(
                  works("eval t = now() | timechart timefield=t span=1h count() | timewrap 1d"))));

  /** Commands of the grammar's {@code commands} rule left out, with the reason. */
  private static final Map<String, String> COMMANDS_NOT_COVERED =
      Map.of("kmeansCommand", "kmeans stands for the knn command in these tests");

  /** Commands that are refused after knn whatever their arguments. */
  private static final Set<String> COMMANDS_ALWAYS_REFUSED =
      Set.of("adCommand", "mlCommand"); // unsupported in Calcite

  /**
   * The commands of the grammar's {@code pplCommands} rule, which start a query and so never come
   * after a k-NN step: a query with each, as an input combined with a k-NN input or before one, or
   * the reason it is left out.
   */
  private static final Map<String, String> SOURCE_COMMAND_QUERIES =
      Map.of(
          "searchCommand", "search source=VECS doc_id=1 | kmeans centroids=2",
          "multisearchCommand",
              "| multisearch [ source=VECS | kmeans centroids=2 ] [ source=DOCS ]",
          "unionCommand", "| union [ source=VECS | kmeans centroids=2 ] [ source=DOCS ]",
          "makeresultsCommand",
              "| makeresults count=1 | append [ source=VECS | kmeans centroids=2 | fields _score ]",
          "graphLookupCommand",
              "| graphLookup DOCS start=1 edge=doc_id-->doc_id as g | append [ source=VECS |"
                  + " kmeans centroids=2 | fields _score ]");

  private static final Map<String, String> SOURCE_COMMANDS_NOT_COVERED =
      Map.of(
          "describeCommand", "lists fields, not a query a k-NN step can be part of",
          "restCommand", "calls a REST endpoint, not a query a k-NN step can be part of",
          "showDataSourcesCommand", "lists data sources, not a query a k-NN step can be part of");

  /** The alternatives of a grammar rule, from the parser's context class for it. */
  private static Set<String> grammarAlternatives(Class<?> ruleContext) {
    Set<String> alternatives = new TreeSet<>();
    for (Method method : ruleContext.getDeclaredMethods()) {
      if (method.getParameterCount() == 0
          && ParserRuleContext.class.isAssignableFrom(method.getReturnType())) {
        alternatives.add(method.getName());
      }
    }
    return alternatives;
  }

  /**
   * The {@code _score} invariant as a property of every command of the grammar: each query runs
   * after a k-NN step, and in an input without knn combined with a k-NN input, and before one, by
   * each command that combines inputs (append, join, multisearch, union, appendcol), and has the
   * outcome it declares there: a plan whose {@code _score} columns are all the k-NN score (traced
   * to {@code KNN_SCORE}, {@code REAL}) and that has no {@code _score0}-like column, the k-NN error
   * named, or the refusal of another check named. Every command has a query that works after knn,
   * unless it is always refused.
   */
  @Test
  public void everyCommandKeepsTheKnnScoreInvariant() {
    doReturn(10).when(settings).getSettingValue(Key.PPL_REX_MAX_MATCH_LIMIT);
    doReturn("simple_pattern").when(settings).getSettingValue(Key.PATTERN_METHOD);
    doReturn("label").when(settings).getSettingValue(Key.PATTERN_MODE);
    doReturn(10).when(settings).getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT);
    doReturn(100000).when(settings).getSettingValue(Key.PATTERN_BUFFER_LIMIT);
    doReturn(false).when(settings).getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN);
    Set<String> covered = new TreeSet<>(COMMAND_CASES.keySet());
    covered.addAll(COMMANDS_NOT_COVERED.keySet());
    assertEquals(
        "commands of the grammar",
        grammarAlternatives(OpenSearchPPLParser.CommandsContext.class),
        covered);
    Set<String> sourceCovered = new TreeSet<>(SOURCE_COMMAND_QUERIES.keySet());
    sourceCovered.addAll(SOURCE_COMMANDS_NOT_COVERED.keySet());
    assertEquals(
        "source commands of the grammar",
        grammarAlternatives(OpenSearchPPLParser.PplCommandsContext.class),
        sourceCovered);

    List<String> failures = new ArrayList<>();
    for (String query : SOURCE_COMMAND_QUERIES.values()) {
      check(query, PLAN, "", failures);
    }
    for (Map.Entry<String, List<Case>> command : COMMAND_CASES.entrySet()) {
      String table = command.getKey().equals("flattenCommand") ? "OBJS" : "VECS";
      boolean worksAfterKnn = false;
      for (Case c : command.getValue()) {
        worksAfterKnn |= c.afterKnn().kind() == Outcome.Kind.PLAN;
        for (Place place : Place.values()) {
          Outcome expected = c.expected(place);
          if (expected.kind() != Outcome.Kind.SKIP) {
            check(place.query(c.rest(), table), expected, place.command(), failures);
          }
        }
      }
      if (!worksAfterKnn && !COMMANDS_ALWAYS_REFUSED.contains(command.getKey())) {
        failures.add(command.getKey() + ": no query works after knn");
      }
    }
    assertTrue(failures.size() + " failures:\n" + String.join("\n", failures), failures.isEmpty());
  }

  /** Add to {@code failures} if lowering {@code ppl} does not have the expected outcome. */
  private void check(String ppl, Outcome expected, String combiner, List<String> failures) {
    String message =
        expected.message() == null ? null : expected.message().replace(COMBINER, combiner);
    RelNode plan;
    try {
      plan = knnPlan(ppl);
    } catch (RuntimeException | AssertionError e) {
      Throwable cause = e instanceof ErrorReport report ? report.getCause() : e;
      boolean knnError =
          cause instanceof SemanticCheckException semantic && semantic.isWithoutCalciteFallback();
      boolean clientError =
          cause instanceof QueryEngineException || cause instanceof IllegalArgumentException;
      Outcome.Kind actual =
          knnError ? Outcome.Kind.KNN_ERROR : clientError ? Outcome.Kind.REFUSAL : null;
      if (actual != expected.kind() || !cause.getMessage().equals(message)) {
        failures.add(
            ppl + "\n    expected " + expected.kind() + " " + message + "\n    got " + cause);
      }
      return;
    }
    if (expected.kind() != Outcome.Kind.PLAN) {
      failures.add(ppl + "\n    expected " + expected.kind() + " " + message + "\n    got a plan");
    }
    for (RelDataTypeField field : plan.getRowType().getFieldList()) {
      if (field.getName().equals("_score")
          && !KnnPlanBuilder.isKnnScoreColumn(plan, field.getIndex())) {
        failures.add(ppl + "\n    _score is not the k-NN score: " + plan.getRowType());
      }
      if (field.getName().matches("_score[0-9]+")) {
        failures.add(ppl + "\n    has " + field.getName() + ": " + plan.getRowType());
      }
    }
  }

  /** A scannable table whose columns are followed by the metadata columns _id and _score. */
  private static class MetadataTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;
    private final List<String> names;
    private final List<RelDataType> types;

    MetadataTable(ImmutableList<Object[]> rows, List<String> names, List<RelDataType> types) {
      this.rows = rows;
      this.names = names;
      this.types = types;
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(
          rows.stream()
              .map(
                  r -> {
                    Object[] row = Arrays.copyOf(r, r.length + 2);
                    row[r.length] = "id" + r[0];
                    row[r.length + 1] = 1.0f;
                    return row;
                  })
              .toList());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = TYPE_FACTORY.builder();
      for (int i = 0; i < names.size(); i++) {
        builder.add(names.get(i), TYPE_FACTORY.createTypeWithNullability(types.get(i), true));
      }
      builder.add("_id", SqlTypeName.VARCHAR).nullable(true);
      builder.add("_score", SqlTypeName.REAL).nullable(true);
      return builder.build();
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
