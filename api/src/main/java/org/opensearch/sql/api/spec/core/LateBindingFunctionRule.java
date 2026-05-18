/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.core;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;

/**
 * Binds custom function implementations at compilation time by rewriting to executable Calcite
 * expressions.
 */
class LateBindingFunctionRule extends RelHomogeneousShuttle {

  /** Operator-to-impl mappings collected from all function specs. */
  private final Map<SqlOperator, BiFunction<RexBuilder, RexCall, RexNode>> bindings =
      UnifiedFunctionSpec.ALL_SPECS.values().stream()
          .filter(spec -> spec.getImpl() != null)
          .collect(
              Collectors.toMap(UnifiedFunctionSpec::getOperator, UnifiedFunctionSpec::getImpl));

  @Override
  public RelNode visit(RelNode node) {
    RelNode visited = super.visit(node);
    RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    return visited.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            RexCall visited = (RexCall) super.visitCall(call);
            return Optional.ofNullable(bindings.get(visited.getOperator()))
                .map(impl -> impl.apply(rexBuilder, visited))
                .orElse(visited);
          }
        });
  }
}
