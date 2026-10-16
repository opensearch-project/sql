/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.allFields;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.head;
import static org.opensearch.sql.ast.dsl.AstDSL.in;
import static org.opensearch.sql.ast.dsl.AstDSL.inSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.knn;
import static org.opensearch.sql.ast.dsl.AstDSL.knnFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.or;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.vectorLiteral;
import static org.opensearch.sql.calcite.knn.KnnTestSchema.explain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.KnnFunction;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Knn;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Union;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Lowering of both k-NN forms through {@link CalciteRelNodeVisitor#analyzeStatement}: the rows of
 * the worked examples of the design (form A and form B give the same plan), the errors, and the
 * {@code _score} column rule.
 */
class KnnLoweringTest {

  private static final String SCAN = "LogicalTableScan(table=[[products]])";

  /** The canonical k-NN Project over products, with no options. */
  private static final String SCORE_PROJECT =
      "LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3], embedding=[$4],"
          + " _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL, 0.33E0:REAL)%s)])";

  private final CalciteRelNodeVisitor visitor =
      new CalciteRelNodeVisitor(mock(DataSourceService.class));

  // ------------------------------------------------------------------ AST builders

  private static UnresolvedExpression query() {
    return vectorLiteral(new BigDecimal("0.12"), new BigDecimal("0.40"), new BigDecimal("0.33"));
  }

  /** Form A: {@code knn(embedding, [0.12, 0.40, 0.33], options...)}. */
  private static KnnFunction knnCall(UnresolvedArgument... options) {
    return knnFunction(field("embedding"), query(), options);
  }

  /** Form B: {@code knn field=embedding vector=[0.12, 0.40, 0.33] options...}. */
  private static Knn knnCommand(UnresolvedPlan input, Argument... options) {
    return knn(input, field("embedding"), query(), options);
  }

  private static UnresolvedArgument kArg(int k) {
    return unresolvedArg("k", intLiteral(k));
  }

  private static Argument k(int k) {
    return argument("k", intLiteral(k));
  }

  private static Field descending(String name) {
    return field(name, argument("asc", booleanLiteral(false)));
  }

  private static UnresolvedExpression categoryIsX() {
    return compare("=", field("category"), stringLiteral("x"));
  }

  private static UnresolvedExpression cheap() {
    return compare("<", field("price"), intLiteral(100));
  }

  private static UnresolvedPlan excludeMeta(UnresolvedPlan plan) {
    return project(plan, AllFieldsExcludeMeta.of());
  }

  // ------------------------------------------------------------------ helpers

  /**
   * Lower a statement, then lower the same AST again in a fresh context, as the profiling path of
   * {@code QueryService} does, and check both give the same plan: lowering changes the AST (it
   * attaches nodes, resolves k), so a second pass must not see a different statement.
   */
  private RelNode plan(UnresolvedPlan statement) {
    RelNode first = lowerOnce(statement);
    assertEquals(
        explain(first), explain(lowerOnce(statement)), "lowering the same AST again differs");
    return first;
  }

  private RelNode lowerOnce(UnresolvedPlan statement) {
    CalcitePlanContext context = KnnTestSchema.newContext();
    visitor.analyzeStatement(statement, context);
    return context.relBuilder.build();
  }

  private String failure(UnresolvedPlan statement) {
    return assertThrows(SemanticCheckException.class, () -> plan(statement)).getMessage();
  }

  /** Lower both forms, check they give the same plan, and return it. */
  private String samePlan(UnresolvedPlan formA, UnresolvedPlan formB) {
    String a = explain(plan(formA));
    assertEquals(a, explain(plan(formB)), "form A and form B give different plans");
    return a;
  }

  private static String scoreProject(String options) {
    return String.format(SCORE_PROJECT, options);
  }

  // ------------------------------------------------------------------ worked examples

  /** {@code source=t | where knn(e, [...]) | head 10}: k = 10. */
  @Test
  void derivedKFromHead() {
    String plan =
        samePlan(
            head(filter(relation("products"), knnCall()), 10, 0),
            head(knnCommand(relation("products")), 10, 0));
    assertEquals(
        "LogicalSort(fetch=[10])\n"
            + "  LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "    "
            + scoreProject("")
            + "\n"
            + "      "
            + SCAN
            + "\n",
        plan);
  }

  /** {@code ... | fields name, _score | sort - _score | head 10}: k = 10 through both. */
  @Test
  void derivedKThroughFieldsAndSortByScore() {
    String plan =
        samePlan(
            head(
                sort(
                    project(
                        filter(relation("products"), knnCall()), field("name"), field("_score")),
                    descending("_score")),
                10,
                0),
            head(
                sort(
                    project(knnCommand(relation("products")), field("name"), field("_score")),
                    descending("_score")),
                10,
                0));
    assertEquals(
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "  LogicalProject(name=[$1], _score=[$6])\n"
            + "    LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "      "
            + scoreProject("")
            + "\n"
            + "        "
            + SCAN
            + "\n",
        plan);
  }

  /** {@code where c='x' and knn(e, [...], k=50) | sort - price | head 10}. */
  @Test
  void explicitKWithPreFilterThenAnotherSort() {
    String plan =
        samePlan(
            head(
                sort(
                    filter(relation("products"), and(categoryIsX(), knnCall(kArg(50)))),
                    descending("price")),
                10,
                0),
            head(
                sort(
                    knnCommand(filter(relation("products"), categoryIsX()), k(50)),
                    descending("price")),
                10,
                0));
    assertEquals(
        "LogicalSort(sort0=[$2], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "  LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[50])\n"
            + "    "
            + scoreProject(", MAP('k', 50)")
            + "\n"
            + "      LogicalFilter(condition=[=($3, 'x')])\n"
            + "        "
            + SCAN
            + "\n",
        plan);
  }

  /** {@code knn field=e vector=[...] k=50 | where price < 100}: a post-filter over 50 rows. */
  @Test
  void explicitKThenPostFilter() {
    String plan =
        samePlan(
            filter(filter(relation("products"), knnCall(kArg(50))), cheap()),
            filter(knnCommand(relation("products"), k(50)), cheap()));
    assertEquals(
        "LogicalFilter(condition=[<($2, 100)])\n"
            + "  LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[50])\n"
            + "    "
            + scoreProject(", MAP('k', 50)")
            + "\n"
            + "      "
            + SCAN
            + "\n",
        plan);
  }

  /** The pre-filter is every other conjunct, wherever knn() stands among them. */
  @Test
  void preFilterIsTheOtherConjuncts() {
    samePlan(
        filter(relation("products"), and(and(knnCall(kArg(5)), categoryIsX()), cheap())),
        knnCommand(filter(relation("products"), and(categoryIsX(), cheap())), k(5)));
  }

  /** An explicit k stays a MAP('k') option when a following head lowers the row count. */
  @Test
  void explicitKIsKeptAfterAFollowingHead() {
    String plan =
        samePlan(
            head(
                filter(
                    relation("products"),
                    knnCall(kArg(50), unresolvedArg("ef_search", intLiteral(100)))),
                10,
                0),
            head(
                knnCommand(relation("products"), k(50), argument("ef_search", intLiteral(100))),
                10,
                0));
    assertEquals(
        "LogicalSort(fetch=[10])\n"
            + "  LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[50])\n"
            + "    "
            + scoreProject(", MAP('k', 50), MAP('ef_search', 100)")
            + "\n"
            + "      "
            + SCAN
            + "\n",
        plan);
  }

  /** A reference-only projection may stand between the source and the k-NN step. */
  @Test
  void fieldsBeforeTheStep() {
    String plan =
        samePlan(
            head(
                filter(
                    project(relation("products"), field("doc_id"), field("embedding")), knnCall()),
                3,
                0),
            head(
                knnCommand(project(relation("products"), field("doc_id"), field("embedding"))),
                3,
                0));
    assertEquals(
        "LogicalSort(fetch=[3])\n"
            + "  LogicalSort(sort0=[$2], dir0=[DESC-nulls-last], fetch=[3])\n"
            + "    LogicalProject(doc_id=[$0], embedding=[$4], _score=[KNN_SCORE($4,"
            + " ARRAY(0.12E0:REAL, 0.4E0:REAL, 0.33E0:REAL))])\n"
            + "      "
            + SCAN
            + "\n",
        plan);
  }

  // ------------------------------------------------------------------ errors

  /** {@code where knn(e, [...]) | sort - price | head 10}: missing k at sort. */
  @Test
  void missingKAtAnotherSort() {
    String expected =
        "knn requires k or a following head; k cannot be derived from a head after [sort], set k"
            + " explicitly, e.g. k=10";
    assertEquals(
        expected,
        failure(head(sort(filter(relation("products"), knnCall()), descending("price")), 10, 0)));
    assertEquals(
        expected,
        failure(head(sort(knnCommand(relation("products")), descending("price")), 10, 0)));
  }

  /** {@code knn field=e vector=[...] | where price < 100 | head 10}: missing k at where. */
  @Test
  void missingKAtAPostFilter() {
    String expected =
        "knn requires k or a following head; k cannot be derived from a head after [where], set k"
            + " explicitly, e.g. k=10";
    assertEquals(expected, failure(head(filter(knnCommand(relation("products")), cheap()), 10, 0)));
    assertEquals(
        expected, failure(head(filter(filter(relation("products"), knnCall()), cheap()), 10, 0)));
  }

  /** {@code where c='x' | knn field=e vector=[...] | stats count() by brand}: no head at all. */
  @Test
  void missingKBeforeStats() {
    UnresolvedPlan statement =
        agg(
            knnCommand(filter(relation("products"), categoryIsX())),
            exprList(alias("count()", aggregate("count", allFields()))),
            List.of(),
            exprList(alias("name", field("name"))),
            defaultStatsArgs());
    assertEquals(
        "knn requires k or a following head; k cannot be derived from a head after [stats], set k"
            + " explicitly, e.g. k=10",
        failure(statement));
  }

  /** {@code where c='x' or knn(e, [...], k=10)}. */
  @Test
  void knnUnderOr() {
    assertEquals(
        KnnErrors.KNN_UNDER_OR_OR_NOT_MESSAGE,
        failure(filter(relation("products"), or(categoryIsX(), knnCall(kArg(10))))));
  }

  /** {@code where doc_id in (1, knn(...))}: knn in the value list of an in is not a conjunct. */
  @Test
  void knnInTheValueListOfAnInIsUnderAnotherOperator() {
    assertEquals(
        KnnErrors.KNN_UNDER_OR_OR_NOT_MESSAGE,
        failure(
            filter(relation("products"), in(field("doc_id"), intLiteral(1), knnCall(kArg(10))))));
  }

  /** {@code eval z = price + 1 | where knn(e, [...], k=10)}: knn placement, in both forms. */
  @Test
  void placementAfterAComputedColumn() {
    UnresolvedPlan evalA =
        eval(relation("products"), let(field("z"), function("+", field("price"), intLiteral(1))));
    UnresolvedPlan evalB =
        eval(relation("products"), let(field("z"), function("+", field("price"), intLiteral(1))));
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, failure(filter(evalA, knnCall(kArg(10)))));
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, failure(knnCommand(evalB, k(10))));
  }

  @Test
  void placementAfterAHead() {
    assertEquals(
        KnnErrors.KNN_PLACEMENT_MESSAGE,
        failure(knnCommand(head(relation("products"), 100, 0), k(10))));
  }

  @Test
  void moreThanOneKnnInABranch() {
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(knnCommand(knnCommand(relation("products"), k(10)), k(5))));
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(filter(filter(relation("products"), knnCall(kArg(10))), knnCall(kArg(5)))));
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(filter(relation("products"), and(knnCall(kArg(10)), knnCall(kArg(5))))));
  }

  @Test
  void argumentErrorsAreTheSameInBothForms() {
    assertEquals(
        "knn field [name] is not a knn_vector field",
        failure(filter(relation("products"), knnFunction(field("name"), query(), kArg(5)))));
    assertEquals(
        "knn field [name] is not a knn_vector field",
        failure(knn(relation("products"), field("name"), query(), k(5))));
    assertEquals(
        "vector has [2] dimensions; field [embedding] has [3]",
        failure(knn(relation("products"), field("embedding"), vectorLiteral(1, 2), k(5))));
    assertEquals(
        "unknown knn option [rescore]",
        failure(knnCommand(relation("products"), k(5), argument("rescore", booleanLiteral(true)))));
    assertEquals(
        KnnErrors.BINARY_VECTORS_NOT_SUPPORTED_MESSAGE,
        failure(knn(relation("binary_vectors"), field("embedding"), vectorLiteral(1, 2), k(5))));
  }

  /** knn() is only lowered as a where conjunct; elsewhere the expression lowering rejects it. */
  @Test
  void knnFunctionOutsideWhereIsAPlacementError() {
    assertEquals(
        KnnErrors.KNN_PLACEMENT_MESSAGE,
        failure(eval(relation("products"), let(field("s"), knnCall(kArg(5))))));
  }

  /** {@code where doc_id in [ ... ] and knn(...)} and its form B: not pushable yet. */
  @Test
  void aSubsearchInThePreFilterIsNotPushable() {
    String expected =
        "pre-filter [IN(doc_id, [subsearch])] cannot be pushed into the k-NN filter; move it after"
            + " knn as a post-filter";
    assertEquals(
        expected,
        failure(
            filter(
                relation("products"),
                and(
                    inSubquery(project(relation("docs"), field("doc_id")), field("doc_id")),
                    knnCall(kArg(5))))));
    assertEquals(
        expected,
        failure(
            knnCommand(
                filter(
                    relation("products"),
                    inSubquery(project(relation("docs"), field("doc_id")), field("doc_id"))),
                k(5))));
  }

  /** A plain Function named knn, as a parser without a dedicated rule builds it, also works. */
  @Test
  void aPlainKnnFunctionIsLoweredLikeTheKnnFunction() {
    samePlan(
        head(
            filter(
                relation("products"),
                and(categoryIsX(), function("knn", field("embedding"), query(), kArg(7)))),
            3,
            0),
        head(knnCommand(filter(relation("products"), categoryIsX()), k(7)), 3, 0));
  }

  /** {@code knn ... | knn ... | head 10}: more than one knn, not a missing k at the upper one. */
  @Test
  void aSecondStepIsMoreThanOneKnnEvenWithoutK() {
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(head(knnCommand(knnCommand(relation("products"))), 10, 0)));
  }

  /** A KNN_SCORE that no rule pushed down fails at execution with a user error (HTTP 400). */
  @Test
  void executingAnUnpushedKnnScoreFailsWithAUserError() {
    CalcitePlanContext context = KnnTestSchema.newContext();
    visitor.analyzeStatement(head(knnCommand(relation("products")), 10, 0), context);
    RelNode plan = context.relBuilder.build();
    ErrorReport report =
        assertThrows(ErrorReport.class, () -> OpenSearchRelRunners.run(context, plan));
    assertInstanceOf(SemanticCheckException.class, report.getCause());
    assertEquals(KnnErrors.NOT_PUSHED_DOWN_MESSAGE, report.getCause().getMessage());
  }

  /** The recursive analyze() does not resolve k: statements go through analyzeStatement. */
  @Test
  void analyzeWithoutTheStatementPassFailsLoudly() {
    CalcitePlanContext context = KnnTestSchema.newContext();
    UnresolvedPlan statement = knnCommand(relation("products"), k(5));
    assertThrows(IllegalStateException.class, () -> visitor.analyze(statement, context));
  }

  // ------------------------------------------------------------------ _score rule

  /** Without fields, a k-NN branch shows _score but still hides the other metadata columns. */
  @Test
  void scoreIsVisibleByDefault() {
    RelNode plan = plan(project(knnCommand(relation("products"), k(5)), allFields()));
    assertEquals(
        List.of("doc_id", "name", "price", "category", "embedding", "_score"),
        plan.getRowType().getFieldNames());
  }

  @Test
  void otherBranchesStillHideScore() {
    RelNode plan = plan(project(relation("products"), allFields()));
    assertEquals(
        List.of("doc_id", "name", "price", "category", "embedding"),
        plan.getRowType().getFieldNames());
  }

  @Test
  void wildcardFieldsIncludeScoreInAKnnBranch() {
    RelNode plan = plan(project(knnCommand(relation("products"), k(5)), field("*")));
    assertEquals(
        List.of("doc_id", "name", "price", "category", "embedding", "_score"),
        plan.getRowType().getFieldNames());
  }

  /** {@code <left> | join left=v right=d on v.doc_id = d.doc_id <right>}, as PPL parses it. */
  private static UnresolvedPlan aliasedJoin(UnresolvedPlan left, UnresolvedPlan right) {
    return new Join(
            new SubqueryAlias("d", right),
            Optional.of("v"),
            Optional.of("d"),
            Join.JoinType.INNER,
            Optional.of(
                compare(
                    "=", field(qualifiedName("v", "doc_id")), field(qualifiedName("d", "doc_id")))),
            new Join.JoinHint(),
            Optional.empty(),
            Argument.ArgumentMap.empty())
        .attach(left);
  }

  /**
   * The join input with the k-NN step keeps its _score ($5 of the join, the k-NN score); the other
   * input's own was stripped by the projection the parser puts on join inputs.
   */
  @Test
  void scoreSurvivesAJoinInput() {
    UnresolvedPlan vectors = excludeMeta(knnCommand(relation("products"), k(10)));
    UnresolvedPlan docs = excludeMeta(relation("docs"));
    assertEquals(
        "LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3], embedding=[$4],"
            + " _score=[$5], d.doc_id=[$6], title=[$7])\n"
            + "  LogicalJoin(condition=[=($0, $6)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _score=[$6])\n"
            + "      LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 10))])\n"
            + "          LogicalTableScan(table=[[products]])\n"
            + "    LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "      LogicalProject(doc_id=[$0], title=[$1])\n"
            + "        LogicalTableScan(table=[[docs]])\n",
        explain(plan(project(aliasedJoin(vectors, docs), allFields()))));
  }

  /**
   * Scenario 4 of the design: {@code source=products | knn ... k=10 | join left=v right=d on
   * v.doc_id = d.doc_id docs | fields d.title, v._score}.
   */
  @Test
  void vectorIndexJoinedBack() {
    UnresolvedPlan statement =
        project(
            aliasedJoin(
                excludeMeta(knnCommand(relation("products"), k(10))),
                excludeMeta(relation("docs"))),
            field(qualifiedName("d", "title")),
            field(qualifiedName("v", "_score")));
    assertEquals(
        "LogicalProject(title=[$7], _score=[$5])\n"
            + "  LogicalJoin(condition=[=($0, $6)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _score=[$6])\n"
            + "      LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        "
            + scoreProject(", MAP('k', 10)")
            + "\n"
            + "          "
            + SCAN
            + "\n"
            + "    LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "      LogicalProject(doc_id=[$0], title=[$1])\n"
            + "        LogicalTableScan(table=[[docs]])\n",
        explain(plan(statement)));
  }

  /** The k-NN branch may also be the right input of the join: _score ($7) is its k-NN score. */
  @Test
  void scoreSurvivesTheRightJoinInput() {
    UnresolvedPlan vectors = excludeMeta(knnCommand(relation("products"), k(10)));
    UnresolvedPlan docs = excludeMeta(relation("docs"));
    assertEquals(
        "LogicalProject(doc_id=[$0], title=[$1], d.doc_id=[$2], name=[$3], price=[$4],"
            + " category=[$5], embedding=[$6], _score=[$7])\n"
            + "  LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1])\n"
            + "      LogicalTableScan(table=[[docs]])\n"
            + "    LogicalSystemLimit(sort0=[$5], dir0=[DESC-nulls-last], fetch=[50000],"
            + " type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "      LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _score=[$6])\n"
            + "        LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "          LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 10))])\n"
            + "            LogicalTableScan(table=[[products]])\n",
        explain(plan(project(aliasedJoin(docs, vectors), allFields()))));
  }

  /**
   * Without the parser's projections on the join inputs, the lowering itself drops the hidden
   * _score of the input without a k-NN step, so _score ($9) is the k-NN score. The visible {@code
   * d._id} is not from the k-NN change: base 44b1a76b6 also renames the right input's {@code _id}
   * (and {@code _score}) to {@code d._id} (and {@code d._score}) in a join of unwrapped inputs, and
   * the top-level metadata stripping only hides the original names.
   */
  @Test
  void theOtherJoinInputLosesItsHiddenScore() {
    UnresolvedPlan statement =
        project(
            aliasedJoin(relation("docs"), knnCommand(relation("products"), k(10))), allFields());
    assertEquals(
        "LogicalProject(doc_id=[$0], title=[$1], d.doc_id=[$3], name=[$4], price=[$5],"
            + " category=[$6], embedding=[$7], d._id=[$8], _score=[$9])\n"
            + "  LogicalJoin(condition=[=($0, $3)], joinType=[inner])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1], _id=[$2])\n"
            + "      LogicalTableScan(table=[[docs]])\n"
            + "    LogicalSystemLimit(sort0=[$6], dir0=[DESC-nulls-last], fetch=[50000],"
            + " type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "      LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 10))])\n"
            + "          LogicalTableScan(table=[[products]])\n",
        explain(plan(statement)));
  }

  /**
   * A subsearch with the k-NN step keeps its _score through the subsearch boundary; the main
   * search's rows get a null _score instead of their hidden one.
   */
  @Test
  void scoreSurvivesASubsearch() {
    UnresolvedPlan subsearch = excludeMeta(knnCommand(relation("products"), k(10)));
    UnresolvedPlan statement = project(new Append(subsearch).attach(relation("docs")), allFields());
    assertEquals(
        "LogicalProject(doc_id=[$0], title=[$1], _score=[$3], name=[$4], price=[$5], category=[$6],"
            + " embedding=[$7])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(doc_id=[$0], title=[$1], _id=[$2], _score=[null:REAL],"
            + " name=[null:VARCHAR], price=[null:DOUBLE], category=[null:VARCHAR],"
            + " embedding=[null:VECTOR(3, FLOAT, l2)])\n"
            + "      LogicalTableScan(table=[[docs]])\n"
            + "    LogicalProject(doc_id=[$0], title=[null:VARCHAR], _id=[null:VARCHAR],"
            + " _score=[$6], name=[$1], price=[$2], category=[$3], embedding=[$4])\n"
            + "      LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "        LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 10))])\n"
            + "          LogicalTableScan(table=[[products]])\n",
        explain(plan(statement)));
  }

  /**
   * multisearch without the parser's projections on its branches: the other branch's hidden _score
   * becomes null, so the combined _score is the k-NN score or null.
   */
  @Test
  void multisearchNullsTheOtherBranchesHiddenScore() {
    UnresolvedPlan statement =
        new Multisearch(List.of(knnCommand(relation("products"), k(2)), relation("docs")));
    assertEquals(
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3], embedding=[$4],"
            + " _id=[$5], _score=[$6], title=[null:VARCHAR])\n"
            + "    LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "      "
            + scoreProject(", MAP('k', 2)")
            + "\n"
            + "        "
            + SCAN
            + "\n"
            + "  LogicalProject(doc_id=[$0], name=[null:VARCHAR], price=[null:DOUBLE],"
            + " category=[null:VARCHAR], embedding=[null:VECTOR(3, FLOAT, l2)], _id=[$2],"
            + " _score=[null:REAL], title=[$1])\n"
            + "    LogicalTableScan(table=[[docs]])\n",
        explain(plan(statement)));
  }

  /**
   * A union of unwrapped inputs combined by position, as SQL does: the other input's hidden _score
   * becomes null in place, so the inputs still line up.
   */
  @Test
  void positionalUnionNullsTheOtherInputsHiddenScoreInPlace() {
    UnresolvedPlan statement =
        new Union(List.of(knnCommand(relation("products"), k(2)), relation("products")), false);
    assertEquals(
        "LogicalUnion(all=[true])\n"
            + "  LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "    "
            + scoreProject(", MAP('k', 2)")
            + "\n"
            + "      "
            + SCAN
            + "\n"
            + "  LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3], embedding=[$4],"
            + " _id=[$5], _score=[null:REAL])\n"
            + "    "
            + SCAN
            + "\n",
        explain(plan(statement)));
  }

  /**
   * After {@code fields - _score} the k-NN input has no _score left, so the other inputs' hidden
   * _score is dropped rather than nulled: the output has no _score column at all.
   */
  @Test
  void withoutAKnnScoreLeftTheOtherInputsDropTheirs() {
    List<UnresolvedPlan> statements =
        List.of(
            project(new Append(relation("docs")).attach(knnWithoutScore()), allFields()),
            project(new Multisearch(List.of(knnWithoutScore(), relation("docs"))), allFields()),
            new Union(List.of(knnWithoutScore(), relation("products")), false),
            project(
                new AppendPipe(
                        projectWithArg(
                            knn(null, field("embedding"), query(), k(2)),
                            exprList(argument("exclude", booleanLiteral(true))),
                            field("_score")))
                    .attach(relation("products")),
                allFields()));
    for (UnresolvedPlan statement : statements) {
      RelNode plan = plan(statement);
      assertFalse(plan.getRowType().getFieldNames().contains("_score"), explain(plan));
    }
  }

  /** {@code source=products | knn ... k=2 | fields - _score}. */
  private static UnresolvedPlan knnWithoutScore() {
    return projectWithArg(
        knnCommand(relation("products"), k(2)),
        exprList(argument("exclude", booleanLiteral(true))),
        field("_score"));
  }

  /** A k-NN step in one join input does not count against a k-NN step in the other. */
  @Test
  void eachJoinInputIsItsOwnBranch() {
    UnresolvedPlan left = excludeMeta(knnCommand(relation("products"), k(10)));
    UnresolvedPlan right =
        excludeMeta(
            knn(relation("byte_vectors"), field("embedding"), vectorLiteral(1, 2, 3), k(5)));
    plan(aliasedJoin(left, right));
  }
}
