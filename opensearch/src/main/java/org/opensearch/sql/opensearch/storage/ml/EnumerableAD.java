/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.ml;

import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.plan.AD;
import org.opensearch.sql.opensearch.storage.NodeClientHolder;

public class EnumerableAD extends AD implements EnumerableRel {

  /**
   * Creates a EnumerableAD operator
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits collation traits of the operator, usually NONE for ad
   * @param input Input relational expression
   * @param arguments an argument mapping of parameter keys and values
   */
  public EnumerableAD(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, Map<String, Object> arguments) {
    super(cluster, traits, input, arguments);
  }

  @Override
  public EnumerableAD copy(RelTraitSet traitSet, RelNode input) {
    return new EnumerableAD(getCluster(), traitSet, input, getArguments());
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();

    // Get child Enumerable
    final Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);

    // Current operator's physical row type
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(inputResult.physType.getFormat()));

    // Child Enumerable object
    final Expression childExpr = builder.append("inputEnumerable", inputResult.block);

    final String[] inputFieldNames = getInput().getRowType().getFieldNames().toArray(new String[0]);
    final String[] outputFieldNames = getRowType().getFieldNames().toArray(new String[0]);

    final Expression inputTypeExpr =
        builder.append("inputRowType", Expressions.constant(inputFieldNames, String[].class));
    final Expression outputTypeExpr =
        builder.append("outputRowType", Expressions.constant(outputFieldNames, String[].class));
    final Expression argsExpr =
        builder.append("args", Expressions.constant(getArguments(), Map.class));
    final Expression clientExpr =
        builder.append("client", Expressions.call(NodeClientHolder.class, "get"));

    final Expression resultExpr =
        builder.append(
            "result",
            Expressions.call(
                ADEnumerableRuntime.class,
                "ad",
                childExpr,
                inputTypeExpr,
                outputTypeExpr,
                argsExpr,
                clientExpr));

    builder.add(Expressions.return_(null, resultExpr));

    return implementor.result(physType, builder.toBlock());
  }
}
