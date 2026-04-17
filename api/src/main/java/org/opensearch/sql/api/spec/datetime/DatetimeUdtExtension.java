/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.LanguageExtension;
import org.opensearch.sql.api.spec.LanguageSpec.PostAnalysisRule;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Bridges PPL's datetime UDT semantics with standard Calcite datetime types in the unified query
 * API, so PPL queries over standard schemas behave the same as PPL queries over OpenSearch.
 */
public class DatetimeUdtExtension implements LanguageExtension {

  @Override
  public List<PostAnalysisRule> postAnalysisRules() {
    return List.of(new CoercionRule());
  }

  /**
   * Wraps every standard DATE/TIME/TIMESTAMP expression with {@code CAST(x AS <UDT>)}. UDT
   * expressions (already backed by the same base type) are left alone.
   */
  static class CoercionRule implements PostAnalysisRule {

    /** Standard datetime type → corresponding PPL UDT. */
    private static final Map<SqlTypeName, RelDataType> STD_TO_UDT =
        Map.of(
            SqlTypeName.DATE, TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE),
            SqlTypeName.TIME, TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME),
            SqlTypeName.TIMESTAMP, TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP));

    @Override
    public RelNode apply(RelNode plan) {
      return plan.accept(
          new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
              RelNode visited = super.visit(other);
              RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
              return visited.accept(
                  new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef ref) {
                      return wrap(ref);
                    }

                    @Override
                    public RexNode visitLiteral(RexLiteral literal) {
                      return wrap(literal);
                    }

                    @Override
                    public RexNode visitCall(RexCall call) {
                      return wrap(super.visitCall(call));
                    }

                    private RexNode wrap(RexNode node) {
                      RelDataType udt = STD_TO_UDT.get(node.getType().getSqlTypeName());
                      if (udt == null) {
                        return node;
                      }
                      return rexBuilder.makeCast(
                          rexBuilder
                              .getTypeFactory()
                              .createTypeWithNullability(udt, node.getType().isNullable()),
                          node);
                    }
                  });
            }
          });
    }
  }
}
