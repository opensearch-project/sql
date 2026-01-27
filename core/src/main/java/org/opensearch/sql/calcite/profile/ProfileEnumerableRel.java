/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.GotoExpressionKind;
import org.apache.calcite.linq4j.tree.GotoStatement;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.monitor.profile.ProfilePlanNode;
import org.opensearch.sql.monitor.profile.ProfilePlanNodeMetrics;

/** EnumerableRel wrapper that records inclusive time and row counts. */
public class ProfileEnumerableRel extends AbstractRelNode implements EnumerableRel {

  private final EnumerableRel delegate;
  private final List<RelNode> inputs;
  protected final ProfilePlanNode planNode;

  public ProfileEnumerableRel(
      EnumerableRel delegate, List<RelNode> inputs, ProfilePlanNode planNode) {
    super(
        Objects.requireNonNull(delegate, "delegate").getCluster(),
        Objects.requireNonNull(delegate, "delegate").getTraitSet());
    this.delegate = delegate;
    this.inputs = new ArrayList<>(Objects.requireNonNull(inputs, "inputs"));
    this.planNode = Objects.requireNonNull(planNode, "planNode");
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ProfileEnumerableRel(delegate, inputs, planNode);
  }

  @Override
  protected RelDataType deriveRowType() {
    return delegate.getRowType();
  }

  @Override
  public String getRelTypeName() {
    return delegate.getRelTypeName();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter writer = pw;
    for (RelNode input : inputs) {
      writer = writer.input("input", input);
    }
    return writer;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    EnumerableRel rewritten = (EnumerableRel) delegate.copy(delegate.getTraitSet(), inputs);
    Result result = rewritten.implement(implementor, pref);
    return new Result(
        wrapBlock(
            result.block, implementor.stash(planNode.metrics(), ProfilePlanNodeMetrics.class)),
        result.physType,
        result.format);
  }

  /**
   * Rewrite the generated block by wrapping the returned Enumerable with profiling.
   *
   * <p>Example (simplified):
   *
   * <pre>
   * // Before:
   * {
   *   final Enumerable<Object[]> input = ...;
   *   return input;
   * }
   *
   * // After:
   * {
   *   final Enumerable<Object[]> input = ...;
   *   return ProfileEnumerableRel.profile(input, metrics);
   * }
   * </pre>
   */
  private static BlockStatement wrapBlock(BlockStatement block, Expression metricsExpression) {
    List<Statement> statements = block.statements;
    if (statements.isEmpty()) {
      return block;
    }
    Statement last = statements.get(statements.size() - 1);
    // Expect Calcite to end blocks with a return GotoStatement; skip profiling if it doesn't.
    if (!(last instanceof GotoStatement)) {
      return block;
    }
    GotoStatement gotoStatement = (GotoStatement) last;
    // Only rewrite blocks that return an expression; otherwise keep the original block.
    if (gotoStatement.kind != GotoExpressionKind.Return || gotoStatement.expression == null) {
      return block;
    }
    Expression profiled =
        Expressions.call(
            ProfileEnumerableRel.class, "profile", gotoStatement.expression, metricsExpression);
    BlockBuilder builder = new BlockBuilder();
    for (int i = 0; i < statements.size() - 1; i++) {
      builder.add(statements.get(i));
    }
    builder.add(Expressions.return_(gotoStatement.labelTarget, profiled));
    return builder.toBlock();
  }

  public static <T> Enumerable<T> profile(
      Enumerable<T> enumerable, ProfilePlanNodeMetrics metrics) {
    if (metrics == null) {
      return enumerable;
    }
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<T> enumerator() {
        long start = System.nanoTime();
        Enumerator<T> delegate = enumerable.enumerator();
        metrics.addTimeNanos(System.nanoTime() - start);
        return new ProfileEnumerator<>(delegate, metrics);
      }
    };
  }

  private static final class ProfileEnumerator<T> implements Enumerator<T> {
    private final Enumerator<T> delegate;
    private final ProfilePlanNodeMetrics metrics;

    private ProfileEnumerator(Enumerator<T> delegate, ProfilePlanNodeMetrics metrics) {
      this.delegate = delegate;
      this.metrics = metrics;
    }

    @Override
    public T current() {
      return delegate.current();
    }

    @Override
    public boolean moveNext() {
      long start = System.nanoTime();
      try {
        boolean hasNext = delegate.moveNext();
        if (hasNext) {
          metrics.incrementRows();
        }
        return hasNext;
      } finally {
        metrics.addTimeNanos(System.nanoTime() - start);
      }
    }

    @Override
    public void reset() {
      delegate.reset();
    }

    @Override
    public void close() {
      long start = System.nanoTime();
      try {
        delegate.close();
      } finally {
        metrics.addTimeNanos(System.nanoTime() - start);
      }
    }
  }
}
