/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.api.spec.LanguageSpec.PostAnalysisRule;
import org.opensearch.sql.api.spec.datetime.DatetimeUdtExtension.UdtMapping;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.ImplementorUDF.ImplementableUDFunction;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Normalizes UDT types in the plan by replacing UDT return types with standard Calcite types
 * (signature) and wrapping UDF implementors to convert between UDT and standard values
 * (implementation).
 */
public class DatetimeUdtNormalizeRule implements PostAnalysisRule {

  @Override
  public RelNode apply(RelNode plan) {
    RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
    return plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            return super.visit(other).accept(new UdtRexShuttle(rexBuilder));
          }
        });
  }

  private static class UdtRexShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;

    UdtRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visited = (RexCall) super.visitCall(call);

      if (!(visited.getOperator() instanceof SqlUserDefinedFunction udf
          && udf.getFunction() instanceof ImplementableUDFunction func)) {
        return visited;
      }

      Optional<UdtMapping> returnType = UdtMapping.fromUdtType(visited.getType());
      if (returnType.isEmpty()
          && visited.getOperands().stream()
              .noneMatch(op -> UdtMapping.fromStdType(op.getType()).isPresent())) {
        return visited;
      }

      RelDataType normReturnType = normalizeReturnType(rexBuilder, visited, returnType);
      NotNullImplementor normFuncImpl = normalizeImplementation(visited, func, returnType);
      SqlUserDefinedFunction normUdf =
          new ImplementorUDF(normFuncImpl, func.getNullPolicy()) {
            @Override
            public SqlReturnTypeInference getReturnTypeInference() {
              return returnType
                  .<SqlReturnTypeInference>map(u -> ReturnTypes.explicit(normReturnType))
                  .orElse(udf.getReturnTypeInference());
            }

            @Override
            public UDFOperandMetadata getOperandMetadata() {
              return (UDFOperandMetadata) udf.getOperandTypeChecker();
            }
          }.toUDF(udf.getName(), udf.isDeterministic());
      return rexBuilder.makeCall(normReturnType, normUdf, visited.getOperands());
    }
  }

  /** Replace UDT return type with standard Calcite type. */
  private static RelDataType normalizeReturnType(
      RexBuilder rexBuilder, RexCall call, Optional<UdtMapping> returnUdt) {
    return returnUdt
        .map(u -> u.toStdType(rexBuilder, call.getType().isNullable()))
        .orElse(call.getType());
  }

  /** Wrap implementor to convert inputs (standard → UDT) and output (UDT → standard). */
  private static NotNullImplementor normalizeImplementation(
      RexCall originalCall, ImplementableUDFunction func, Optional<UdtMapping> returnUdt) {
    return (translator, rexCall, operands) -> {
      List<Expression> converted = new ArrayList<>(operands.size());
      for (int i = 0; i < operands.size(); i++) {
        Expression operand = operands.get(i);
        RelDataType opType = originalCall.getOperands().get(i).getType();
        converted.add(
            UdtMapping.fromStdType(opType).map(u -> u.fromStdValue(operand)).orElse(operand));
      }
      Expression result = func.getNotNullImplementor().implement(translator, rexCall, converted);
      return returnUdt.map(u -> u.toStdValue(result)).orElse(result);
    };
  }
}
