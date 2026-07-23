/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/** Finds and binds scalar subqueries used as runtime {@code search} predicates. */
public final class DynamicSearchPlanBinder {

  private DynamicSearchPlanBinder() {}

  /** Returns the first unbound scalar subquery beneath a query_string query argument. */
  public static Optional<RexSubQuery> find(RelNode plan) {
    AtomicReference<RexSubQuery> found = new AtomicReference<>();
    RexShuttle finder =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            if (found.get() == null && isQueryString(call)) {
              RexNode query = queryArgument(call);
              if (query != null) {
                query.accept(
                    new RexShuttle() {
                      @Override
                      public RexNode visitSubQuery(RexSubQuery subQuery) {
                        found.compareAndSet(null, subQuery);
                        return subQuery;
                      }
                    });
              }
            }
            return found.get() == null ? super.visitCall(call) : call;
          }
        };

    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (found.get() == null) {
          node.accept(finder);
          super.visit(node, ordinal, parent);
        }
      }
    }.go(plan);
    return Optional.ofNullable(found.get());
  }

  /** Replaces one runtime scalar query and folds the surrounding string concatenation. */
  public static RelNode bind(RelNode plan, RexSubQuery target, String compiledQueryString) {
    RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
    RexShuttle queryArgumentBinder =
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (subQuery == target) {
              return stringLiteral(rexBuilder, compiledQueryString);
            }
            return subQuery;
          }

          @Override
          public RexNode visitCall(RexCall call) {
            RexNode visited = super.visitCall(call);
            if (visited instanceof RexCall visitedCall) {
              String constant = constantString(visitedCall);
              if (constant != null) {
                return stringLiteral(rexBuilder, constant);
              }
            }
            return visited;
          }
        };
    RexShuttle binder =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            if (isQueryString(call)) {
              RexNode visited = call.accept(queryArgumentBinder);
              return visited instanceof RexCall visitedCall
                  ? foldQueryStringArgument(visitedCall, rexBuilder)
                  : visited;
            }
            return super.visitCall(call);
          }
        };
    return rewrite(plan, binder);
  }

  private static RelNode rewrite(RelNode node, RexShuttle binder) {
    List<RelNode> inputs = node.getInputs();
    List<RelNode> rewrittenInputs = new ArrayList<>(inputs.size());
    boolean changed = false;
    for (RelNode input : inputs) {
      RelNode rewritten = rewrite(input, binder);
      rewrittenInputs.add(rewritten);
      changed |= rewritten != input;
    }
    RelNode withInputs = changed ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
    return withInputs.accept(binder);
  }

  private static String constantString(RexCall call) {
    if ((call.getKind() == SqlKind.CAST || call.getOperator().getName().equalsIgnoreCase("cast"))
        && call.getOperands().size() == 1) {
      return literalString(call.getOperands().getFirst());
    }
    String name = call.getOperator().getName().toLowerCase(Locale.ROOT);
    if (!(name.equals("concat") || name.equals("||"))) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    for (RexNode operand : call.getOperands()) {
      String value = literalString(operand);
      if (value == null) {
        return null;
      }
      result.append(value);
    }
    return result.toString();
  }

  private static RexNode foldQueryStringArgument(RexCall queryString, RexBuilder rexBuilder) {
    List<RexNode> operands = new ArrayList<>(queryString.getOperands());
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (operand instanceof RexCall map && map.getOperands().size() >= 2) {
        RexNode key = map.getOperands().getFirst();
        if (key instanceof RexLiteral literal
            && "query".equalsIgnoreCase(literal.getValueAs(String.class))) {
          RexNode value = map.getOperands().get(1);
          String constant = literalString(value);
          if (constant != null && !(value instanceof RexLiteral)) {
            List<RexNode> mapOperands = new ArrayList<>(map.getOperands());
            mapOperands.set(1, stringLiteral(rexBuilder, constant));
            operands.set(i, map.clone(map.getType(), mapOperands));
            return queryString.clone(queryString.getType(), operands);
          }
        }
      }
    }
    return queryString;
  }

  private static RexNode stringLiteral(RexBuilder rexBuilder, String value) {
    return rexBuilder.makeLiteral(
        value, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
  }

  private static String literalString(RexNode node) {
    if (node instanceof RexLiteral literal) {
      return literal.getValueAs(String.class);
    }
    if (node instanceof RexCall call) {
      return constantString(call);
    }
    return null;
  }

  private static boolean isQueryString(RexCall call) {
    return call.getOperator().getName().equalsIgnoreCase("query_string");
  }

  private static RexNode queryArgument(RexCall queryString) {
    for (RexNode operand : queryString.getOperands()) {
      if (operand instanceof RexCall map && map.getOperands().size() >= 2) {
        RexNode key = map.getOperands().getFirst();
        if (key instanceof RexLiteral literal
            && "query".equalsIgnoreCase(literal.getValueAs(String.class))) {
          return map.getOperands().get(1);
        }
      }
    }
    return null;
  }
}
