/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.allFields;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.dedupe;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultDedupArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.head;
import static org.opensearch.sql.ast.dsl.AstDSL.inSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.join;
import static org.opensearch.sql.ast.dsl.AstDSL.knn;
import static org.opensearch.sql.ast.dsl.AstDSL.knnFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.limit;
import static org.opensearch.sql.ast.dsl.AstDSL.map;
import static org.opensearch.sql.ast.dsl.AstDSL.not;
import static org.opensearch.sql.ast.dsl.AstDSL.or;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.rename;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.vectorLiteral;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.KnnStep;
import org.opensearch.sql.ast.KnnStep.ResolvedK;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.KnnFunction;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Knn;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.exception.SemanticCheckException;

class KnnLimitResolverTest {

  private static Field descScore() {
    return field("_score", argument("asc", booleanLiteral(false)));
  }

  private static Field ascending(String name) {
    return field(name, argument("asc", booleanLiteral(true)));
  }

  private static Field descending(String name) {
    return field(name, argument("asc", booleanLiteral(false)));
  }

  /** Form B without k. */
  private static Knn knnCommand(UnresolvedPlan input) {
    return knn(input, field("embedding"), vectorLiteral(1, 2, 3));
  }

  /** Form B with an explicit k. */
  private static Knn knnCommand(UnresolvedPlan input, int k) {
    return knn(input, field("embedding"), vectorLiteral(1, 2, 3), argument("k", intLiteral(k)));
  }

  /** Form A without k. */
  private static KnnFunction knnCall() {
    return knnFunction(field("embedding"), vectorLiteral(1, 2, 3));
  }

  /** Form A with an explicit k. */
  private static KnnFunction knnCall(int k) {
    return knnFunction(
        field("embedding"), vectorLiteral(1, 2, 3), unresolvedArg("k", intLiteral(k)));
  }

  /** Idempotency over a statement that lowering changed is covered by the lowering tests. */
  private static ResolvedK resolve(UnresolvedPlan statement, KnnStep step) {
    KnnLimitResolver.resolve(statement);
    return step.getResolvedK();
  }

  /**
   * The error resolving {@code statement} gives: thrown by the pass, or recorded on a step for the
   * lowering to raise (an error about k).
   */
  private static String failure(UnresolvedPlan statement) {
    try {
      KnnLimitResolver.resolve(statement);
    } catch (SemanticCheckException e) {
      return e.getMessage();
    }
    SemanticCheckException recorded =
        steps(statement).stream()
            .map(KnnStep::getUnresolvedK)
            .filter(java.util.Objects::nonNull)
            .findFirst()
            .orElseThrow(() -> new AssertionError("resolving succeeded: " + statement));
    return recorded.getMessage();
  }

  /** Every k-NN step of a statement. */
  private static List<KnnStep> steps(UnresolvedPlan plan) {
    List<KnnStep> steps = new java.util.ArrayList<>();
    if (plan instanceof Knn knn) {
      steps.add(knn);
    }
    if (plan instanceof Filter filter) {
      KnnAst.topLevelKnn(filter.getCondition()).map(KnnAst::knnStep).ifPresent(steps::add);
    }
    KnnAst.subqueryPlans(plan).forEach(subquery -> steps.addAll(steps(subquery)));
    KnnAst.inputs(plan).forEach(input -> steps.addAll(steps(input)));
    return steps;
  }

  private static String missingKAt(String command) {
    return "knn requires k or a following head; k cannot be derived from a head after ["
        + command
        + "], set k explicitly, e.g. k=10";
  }

  // ------------------------------------------------------------------ derived k

  @Test
  void kIsDerivedFromAFollowingHead() {
    KnnFunction knn = knnCall();
    UnresolvedPlan statement = project(head(filter(relation("t"), knn), 10, 0), allFields());
    assertEquals(new ResolvedK(10, false), resolve(statement, knn));

    Knn command = knnCommand(relation("t"));
    assertEquals(new ResolvedK(7, false), resolve(head(command, 7, 0), command));
  }

  @Test
  void derivationPassesFieldsEvalRenameAndSortByScoreDescending() {
    KnnFunction knn = knnCall();
    UnresolvedPlan statement =
        head(
            sort(
                rename(
                    eval(
                        projectWithArg(
                            project(filter(relation("t"), knn), field("name"), field("_score")),
                            exprList(argument("exclude", booleanLiteral(true))),
                            field("name")),
                        let(field("s2"), function("*", field("_score"), intLiteral(2)))),
                    map("s2", "double_score")),
                descScore()),
            10,
            0);
    assertEquals(new ResolvedK(10, false), resolve(statement, knn));
  }

  @Test
  void sortByScoreWithACountIsAHead() {
    Knn command = knnCommand(relation("t"));
    assertEquals(new ResolvedK(5, false), resolve(sort(command, 5, descScore()), command));
  }

  @Test
  void sqlLimitIsLikeHead() {
    KnnFunction knn = knnCall();
    assertEquals(
        new ResolvedK(10, false),
        resolve(
            limit(project(sort(filter(relation("t"), knn), descScore()), allFields()), 10, 0),
            knn));
  }

  @Test
  void derivedKMustBeInRange() {
    assertEquals(
        "k must be between 1 and 10000, but would be derived from [head 20000]; set k explicitly",
        failure(head(knnCommand(relation("t")), 20000, 0)));
  }

  // ------------------------------------------------------------------ missing k

  @Test
  void missingKWithoutAnyHead() {
    assertEquals(KnnErrors.MISSING_K_MESSAGE, failure(knnCommand(relation("t"))));
    assertEquals(
        KnnErrors.MISSING_K_MESSAGE,
        failure(project(filter(relation("t"), knnCall()), allFields())));
  }

  @Test
  void missingKWhenAWhereStopsDerivation() {
    UnresolvedPlan statement =
        head(
            filter(knnCommand(relation("t")), compare("<", field("price"), intLiteral(100))),
            10,
            0);
    assertEquals(missingKAt("where"), failure(statement));
  }

  @Test
  void missingKWhenAnotherSortStopsDerivation() {
    assertEquals(
        missingKAt("sort"),
        failure(head(sort(filter(relation("t"), knnCall()), descending("price")), 10, 0)));
    assertEquals(
        missingKAt("sort"),
        failure(head(sort(knnCommand(relation("t")), ascending("_score")), 10, 0)));
    assertEquals(
        missingKAt("sort"),
        failure(head(sort(knnCommand(relation("t")), descScore(), descending("price")), 10, 0)));
  }

  @Test
  void missingKWhenReverseStopsDerivation() {
    assertEquals(
        missingKAt("reverse"),
        failure(head(new Reverse().attach(knnCommand(relation("t"))), 10, 0)));
  }

  @Test
  void missingKWhenTheHeadHasAnOffset() {
    assertEquals(missingKAt("head 10 from 5"), failure(head(knnCommand(relation("t")), 10, 5)));
    assertEquals(missingKAt("head 10 from 5"), failure(limit(knnCommand(relation("t")), 10, 5)));
  }

  @Test
  void missingKWhenStatsDedupOrEventstatsStopDerivation() {
    UnresolvedPlan stats =
        agg(
            knnCommand(filter(relation("t"), compare("=", field("c"), stringLiteral("x")))),
            exprList(alias("count()", aggregate("count", allFields()))),
            List.of(),
            exprList(alias("brand", field("brand"))),
            defaultStatsArgs());
    assertEquals(missingKAt("stats"), failure(head(stats, 10, 0)));
    assertEquals(
        missingKAt("dedup"),
        failure(
            head(dedupe(knnCommand(relation("t")), defaultDedupArgs(), field("brand")), 10, 0)));
    assertEquals(
        missingKAt("eventstats"),
        failure(
            head(
                new Window(List.of(alias("c", aggregate("count", allFields()))), List.of(), false)
                    .attach(knnCommand(relation("t"))),
                10,
                0)));
  }

  // ------------------------------------------------------------------ explicit k

  @Test
  void explicitKWinsOverAFollowingHead() {
    KnnFunction knn = knnCall(50);
    UnresolvedPlan statement =
        head(
            sort(
                filter(relation("t"), and(compare("=", field("c"), stringLiteral("x")), knn)),
                descending("price")),
            10,
            0);
    assertEquals(new ResolvedK(50, true), resolve(statement, knn));
  }

  @Test
  void explicitKNeedsNoHead() {
    Knn command = knnCommand(relation("t"), 50);
    UnresolvedPlan statement = filter(command, compare("<", field("price"), intLiteral(100)));
    assertEquals(new ResolvedK(50, true), resolve(statement, command));
  }

  @Test
  void explicitKAllowsAHeadUpToK() {
    Knn command = knnCommand(relation("t"), 10);
    assertEquals(new ResolvedK(10, true), resolve(head(command, 5, 5), command));
    Knn bigger = knnCommand(relation("t"), 10);
    assertEquals(new ResolvedK(10, true), resolve(head(bigger, 20, 0), bigger));
  }

  @Test
  void explicitKRejectsAHeadWithOffsetBeyondK() {
    assertEquals(
        "head 5 from 6 needs 11 rows, but knn returns at most k=10; increase k",
        failure(head(knnCommand(relation("t"), 10), 5, 6)));
  }

  @Test
  void explicitKMustBeInRange() {
    assertEquals("k must be between 1 and 10000, got [0]", failure(knnCommand(relation("t"), 0)));
  }

  // ------------------------------------------------------------------ branches

  @Test
  void aHeadAfterAJoinDoesNotReachIntoItsInputs() {
    UnresolvedPlan right = knnCommand(relation("v"));
    UnresolvedPlan statement =
        head(join(right, Join.JoinType.INNER, Optional.empty()).attach(relation("docs")), 10, 0);
    assertEquals(missingKAt("join"), failure(statement));

    UnresolvedPlan left = knnCommand(relation("v"));
    UnresolvedPlan leftSide =
        head(join(relation("docs"), Join.JoinType.INNER, Optional.empty()).attach(left), 10, 0);
    assertEquals(missingKAt("join"), failure(leftSide));
  }

  @Test
  void aHeadInsideABranchIsUsed() {
    Knn command = knnCommand(relation("v"));
    UnresolvedPlan statement =
        join(head(command, 3, 0), Join.JoinType.INNER, Optional.empty()).attach(relation("docs"));
    assertEquals(new ResolvedK(3, false), resolve(statement, command));
  }

  @Test
  void appendAndMultisearchBranchesAreSeparate() {
    UnresolvedPlan append =
        head(new Append(knnCommand(relation("v"))).attach(relation("t")), 10, 0);
    assertEquals(missingKAt("append"), failure(append));

    Knn first = knnCommand(relation("v"), 4);
    Knn second = knnCommand(relation("w"));
    UnresolvedPlan multisearch = head(new Multisearch(List.of(first, head(second, 2, 0))), 10, 0);
    KnnLimitResolver.resolve(multisearch);
    assertEquals(new ResolvedK(4, true), first.getResolvedK());
    assertEquals(new ResolvedK(2, false), second.getResolvedK());
  }

  @Test
  void subsearchesInExpressionsAreSeparateBranches() {
    UnresolvedPlan subsearch = project(knnCommand(relation("v")), field("doc_id"));
    UnresolvedPlan statement =
        head(filter(relation("docs"), inSubquery(subsearch, field("doc_id"))), 10, 0);
    assertEquals(missingKAt("subsearch"), failure(statement));

    Knn inner = knnCommand(relation("v"));
    UnresolvedPlan withHead =
        filter(
            relation("docs"),
            inSubquery(project(head(inner, 5, 0), field("doc_id")), field("doc_id")));
    assertEquals(new ResolvedK(5, false), resolve(withHead, inner));
  }

  // ------------------------------------------------------------------ placement in where

  @Test
  void knnUnderOrOrNotIsRejected() {
    assertEquals(
        KnnErrors.KNN_UNDER_OR_OR_NOT_MESSAGE,
        failure(
            filter(relation("t"), or(compare("=", field("c"), stringLiteral("x")), knnCall(10)))));
    assertEquals(
        KnnErrors.KNN_UNDER_OR_OR_NOT_MESSAGE, failure(filter(relation("t"), not(knnCall(10)))));
  }

  @Test
  void twoKnnConjunctsAreRejected() {
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(filter(relation("t"), and(knnCall(10), knnCall(5)))));
  }

  @Test
  void aPlainKnnFunctionIsNormalisedToCarryK() {
    UnresolvedExpression category = compare("=", field("c"), stringLiteral("x"));
    UnresolvedExpression cheap = compare("<", field("price"), intLiteral(100));
    Filter where =
        (Filter)
            filter(
                relation("t"),
                and(
                    and(category, function("KNN", field("embedding"), vectorLiteral(1, 2, 3))),
                    cheap));
    KnnLimitResolver.resolve(head(where, 10, 0));

    And condition = (And) where.getCondition();
    And left = (And) condition.getLeft();
    assertSame(category, left.getLeft());
    assertSame(cheap, condition.getRight());
    KnnFunction knn = assertInstanceOf(KnnFunction.class, left.getRight());
    assertEquals(new ResolvedK(10, false), knn.getResolvedK());
    assertEquals(List.of(field("embedding"), vectorLiteral(1, 2, 3)), knn.getFuncArgs());
  }

  @Test
  void statementsWithoutKnnAreUntouched() {
    UnresolvedPlan statement = withoutKnn();
    KnnLimitResolver.resolve(statement);
    assertEquals(withoutKnn(), statement);
    Filter where = (Filter) ((Head) statement).getChild().getFirst().getChild().getFirst();
    // KnnFunction equals a Function with the same arguments, so compare classes too.
    assertSame(Function.class, ((Compare) where.getCondition()).getLeft().getClass());
  }

  /** {@code source=t | where abs(a) = 1 | sort - _score | head 1}. */
  private static UnresolvedPlan withoutKnn() {
    return head(
        sort(
            filter(relation("t"), compare("=", function("abs", field("a")), intLiteral(1))),
            descScore()),
        1,
        0);
  }

  // ------------------------------------------------------------------ more than one knn

  /** A second step below the first must not be reported as missing k at the first. */
  @Test
  void aStepBelowAnotherIsMoreThanOneKnn() {
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(head(knnCommand(knnCommand(relation("t"))), 10, 0)));
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(filter(knnCommand(relation("t"), 5), knnCall(3))));
    assertEquals(
        KnnErrors.MORE_THAN_ONE_KNN_MESSAGE,
        failure(
            knnCommand(
                head(
                    join(relation("d"), Join.JoinType.INNER, Optional.empty())
                        .attach(knnCommand(relation("t"))),
                    3,
                    0),
                5)));
  }

  /**
   * The appendpipe sub-pipeline continues the main search, so a k-NN step in it and one in the main
   * search are more than one knn, not a missing k of the main one.
   */
  @Test
  void aStepInAnAppendpipeSubPipelineIsAboveTheMainOne() {
    UnresolvedPlan subPipeline =
        head(new Knn(field("embedding"), vectorLiteral(1, 2, 3), List.of()), 2, 0);
    UnresolvedPlan statement = new AppendPipe(subPipeline).attach(knnCommand(relation("t")));
    assertEquals(KnnErrors.MORE_THAN_ONE_KNN_MESSAGE, failure(statement));

    // Only the main search has one: its k still has to come from above the appendpipe.
    UnresolvedPlan mainOnly = new AppendPipe(head(null, 1, 0)).attach(knnCommand(relation("t")));
    assertEquals(missingKAt("appendpipe"), failure(mainOnly));
  }

  @Test
  void stepsInSeparateBranchesAreNotMoreThanOne() {
    Knn left = knnCommand(relation("t"), 3);
    Knn right = knnCommand(relation("u"), 2);
    KnnLimitResolver.resolve(join(right, Join.JoinType.INNER, Optional.empty()).attach(left));
    assertEquals(new ResolvedK(3, true), left.getResolvedK());
    assertEquals(new ResolvedK(2, true), right.getResolvedK());

    // A subsearch in an expression is a branch of its own, too.
    Knn inner = knnCommand(relation("v"), 2);
    Knn outer =
        knnCommand(
            filter(relation("t"), inSubquery(project(inner, field("doc_id")), field("doc_id"))), 4);
    KnnLimitResolver.resolve(outer);
    assertEquals(new ResolvedK(2, true), inner.getResolvedK());
    assertEquals(new ResolvedK(4, true), outer.getResolvedK());
  }
}
