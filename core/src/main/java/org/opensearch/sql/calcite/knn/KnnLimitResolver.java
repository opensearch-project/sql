/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.KnnStep;
import org.opensearch.sql.ast.KnnStep.ResolvedK;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.KnnFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Knn;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

/**
 * The AST pass that fixes k for every k-NN step of a statement before it is lowered, so the plan
 * never holds a pending top-k boundary. It runs once per statement, on the whole plan.
 *
 * <p>Walking top-down, it carries the nearest {@code head} (PPL) or {@code LIMIT} (SQL) above the
 * current node, through the commands that neither drop nor reorder rows: {@code fields} (including
 * {@code fields -}), {@code eval}, {@code rename}, and a {@code sort} whose only key is {@code
 * _score} descending, a re-statement of the k-NN order. Every other command clears it and is
 * remembered for the error message. The inputs of joins, lookups, appends, multisearch, union and
 * subsearches are separate branches: a {@code head} after them never reaches a k-NN step inside
 * them.
 *
 * <p>At a k-NN step (a {@link Knn} command, or a {@code where} with a {@code knn(...)} conjunct):
 *
 * <ol>
 *   <li>an explicit {@code k=} is used, and a {@code head N from M} it carries must satisfy {@code
 *       M + N <= k};
 *   <li>otherwise k is the size of the carried {@code head}, which must have no offset;
 *   <li>otherwise the query fails with <i>missing k</i>, naming the command that stopped the
 *       derivation.
 * </ol>
 *
 * <p>Errors about k are recorded on the step ({@link KnnStep#setUnresolvedK}) and raised when it is
 * lowered, after the checks of its input, so that a misplaced step reports its placement. The pass
 * is idempotent: lowering may attach nodes under others, and a second pass over the same AST, as
 * the profiling path makes, gives the same result.
 *
 * <p>The result is stored on the step ({@link KnnStep#setResolvedK}). A k-NN step below another one
 * in the same branch fails with <i>more than one knn</i>, before k could mask it as <i>missing
 * k</i>.
 */
public final class KnnLimitResolver {

  private KnnLimitResolver() {}

  /**
   * Resolve k for every k-NN step of a statement: record the k, or why there is none, on the step.
   *
   * @param statement the plan of a whole query statement
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>knn under OR or NOT</i>, <i>more
   *     than one knn</i>, and the argument errors of {@link KnnArguments} and {@link KnnOptions}
   *     (including <i>k out of range</i> for an explicit k)
   */
  public static void resolve(UnresolvedPlan statement) {
    visit(statement, Downstream.NONE, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  /**
   * What stands above the current node, up to the first command that drops or reorders rows.
   *
   * @param headSize the size of the nearest {@code head} above, or {@code null} if none is
   *     reachable
   * @param headOffset that {@code head}'s offset
   * @param stoppedAt the command that made the {@code head} unreachable, or {@code null}
   * @param knnAbove whether a k-NN step above shares the current branch, so another one here is
   *     <i>more than one knn</i>
   */
  private record Downstream(
      @Nullable Integer headSize, int headOffset, @Nullable String stoppedAt, boolean knnAbove) {
    static final Downstream NONE = new Downstream(null, 0, null, false);

    static Downstream head(int size, @Nullable Integer offset, boolean knnAbove) {
      return new Downstream(size, offset == null ? 0 : offset, null, knnAbove);
    }

    static Downstream stoppedAt(String command, boolean knnAbove) {
      return new Downstream(null, 0, command, knnAbove);
    }

    /** The same, for an input that starts a branch of its own. */
    Downstream inSeparateBranch() {
      return new Downstream(headSize, headOffset, stoppedAt, false);
    }

    /** The same, below a k-NN step of the same branch. */
    Downstream withKnnAbove() {
      return new Downstream(headSize, headOffset, stoppedAt, true);
    }
  }

  private static void visit(UnresolvedPlan node, Downstream downstream, Set<Node> visited) {
    // Lowering a statement attaches nodes under others (the main search under an appendpipe
    // sub-pipeline, the source under an appendcol subsearch), so a later pass over the same AST
    // reaches them twice; the first path, through the node's own inputs, is the one that counts.
    if (!visited.add(node)) {
      return;
    }
    Downstream below;
    if (node instanceof Knn knn) {
      resolveStep(knn, KnnArguments.of(knn), downstream);
      below = Downstream.stoppedAt(KnnStep.NAME, true);
    } else if (node instanceof Filter filter && KnnAst.containsKnnCall(filter.getCondition())) {
      Function knn = knnConjunct(filter);
      resolveStep(KnnAst.knnStep(knn), KnnArguments.of(knn), downstream);
      below = Downstream.stoppedAt("where", true);
    } else {
      below = downstreamOfInputs(node, downstream);
    }
    for (UnresolvedPlan subquery : KnnAst.subqueryPlans(node)) {
      visit(subquery, Downstream.stoppedAt("subsearch", false), visited);
    }
    for (UnresolvedPlan input : KnnAst.inputs(node)) {
      visit(input, downstreamOfInput(node, input, below), visited);
    }
  }

  /** What one input of {@code node} sees above it, given what all its inputs do. */
  private static Downstream downstreamOfInput(
      UnresolvedPlan node, UnresolvedPlan input, Downstream below) {
    if (KnnAst.isSeparateBranch(node, input)) {
      return below.inSeparateBranch();
    }
    // The appendpipe sub-pipeline runs on top of the main search: a k-NN step in it is above a
    // k-NN step in the main search, in the same branch.
    if (node instanceof AppendPipe appendPipe
        && input != appendPipe.getSubQuery()
        && KnnAst.subPipelineHasKnnStep(appendPipe)) {
      return below.withKnnAbove();
    }
    return below;
  }

  /**
   * The {@code knn(...)} conjunct of a {@code where}. A plain {@link Function} named {@code knn},
   * as a parser without a dedicated rule builds it, is replaced in the condition by a {@link
   * KnnFunction}, which can carry the resolved k.
   */
  private static Function knnConjunct(Filter filter) {
    Function knn =
        KnnAst.topLevelKnn(filter.getCondition()).orElseThrow(KnnErrors::knnUnderOrOrNot);
    if (knn instanceof KnnFunction) {
      return knn;
    }
    KnnFunction normalised = new KnnFunction(knn.getFuncArgs());
    filter.setCondition(replaceConjunct(filter.getCondition(), knn, normalised));
    return normalised;
  }

  /** {@code condition} with the top-level conjunct {@code target} replaced, its shape kept. */
  private static UnresolvedExpression replaceConjunct(
      UnresolvedExpression condition,
      UnresolvedExpression target,
      UnresolvedExpression replacement) {
    if (condition == target) {
      return replacement;
    }
    if (condition instanceof And and) {
      return new And(
          replaceConjunct(and.getLeft(), target, replacement),
          replaceConjunct(and.getRight(), target, replacement));
    }
    return condition;
  }

  private static void resolveStep(KnnStep step, KnnArguments arguments, Downstream downstream) {
    if (downstream.knnAbove()) {
      throw KnnErrors.moreThanOneKnn();
    }
    Integer explicitK = arguments.parsedOptions().k();
    Integer headSize = downstream.headSize();
    int headOffset = downstream.headOffset();
    // Errors about k depend on the commands above the step; they are recorded and raised when the
    // step is lowered, after the checks of its input, which take precedence.
    if (explicitK != null) {
      if (headSize != null && headOffset > 0 && headOffset + headSize > explicitK) {
        step.setUnresolvedK(KnnErrors.headBeyondK(headSize, headOffset, explicitK));
      } else {
        step.setResolvedK(new ResolvedK(explicitK, true));
      }
    } else if (headSize != null) {
      if (headOffset != 0) {
        step.setUnresolvedK(
            KnnErrors.missingK(
                String.format(Locale.ROOT, "head %d from %d", headSize, headOffset)));
      } else if (!KnnOptions.isValidK(headSize)) {
        step.setUnresolvedK(KnnErrors.derivedKOutOfRange(headSize));
      } else {
        step.setResolvedK(new ResolvedK(headSize, false));
      }
    } else {
      step.setUnresolvedK(KnnErrors.missingK(downstream.stoppedAt()));
    }
  }

  /** What the inputs of a node other than a k-NN step see above them. */
  private static Downstream downstreamOfInputs(UnresolvedPlan node, Downstream downstream) {
    return switch (node) {
      case Head head -> Downstream.head(head.getSize(), head.getFrom(), downstream.knnAbove());
      case Limit limit ->
          Downstream.head(limit.getLimit(), limit.getOffset(), downstream.knnAbove());
      case Project project -> downstream;
      case Eval eval -> downstream;
      case Rename rename -> downstream;
      case Sort sort when isScoreDescending(sort) ->
          sort.getCount() != null && sort.getCount() > 0
              ? Downstream.head(sort.getCount(), 0, downstream.knnAbove())
              : downstream;
      default -> Downstream.stoppedAt(KnnAst.commandName(node), downstream.knnAbove());
    };
  }

  /** Whether a sort's only key is {@code _score}, descending. */
  private static boolean isScoreDescending(Sort sort) {
    if (sort.getSortList().size() != 1) {
      return false;
    }
    Field key = sort.getSortList().getFirst();
    if (!OpenSearchConstants.METADATA_FIELD_SCORE.equals(key.getField().toString())) {
      return false;
    }
    for (Argument argument : key.getFieldArgs()) {
      if ("asc".equals(argument.getArgName())) {
        Literal asc = argument.getValue();
        return Boolean.FALSE.equals(asc.getValue());
      }
    }
    return false;
  }
}
