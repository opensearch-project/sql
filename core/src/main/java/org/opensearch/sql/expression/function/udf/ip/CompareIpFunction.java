/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * {@code compare(ip1, ip2)} compares two IP addresses using a provided op.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(IP, STRING) -> BOOLEAN
 *   <li>(STRING, IP) -> BOOLEAN
 *   <li>(IP, IP) -> BOOLEAN
 * </ul>
 */
public class CompareIpFunction extends ImplementorUDF {
  private final SqlKind kind;

  private CompareIpFunction(SqlKind kind) {
    super(new CompareImplementor(kind), NullPolicy.ANY);
    this.kind = kind;
  }

  public static CompareIpFunction less() {
    return new CompareIpFunction(SqlKind.LESS_THAN);
  }

  public static CompareIpFunction greater() {
    return new CompareIpFunction(SqlKind.GREATER_THAN);
  }

  public static CompareIpFunction lessOrEquals() {
    return new CompareIpFunction(SqlKind.LESS_THAN_OR_EQUAL);
  }

  public static CompareIpFunction greaterOrEquals() {
    return new CompareIpFunction(SqlKind.GREATER_THAN_OR_EQUAL);
  }

  public static CompareIpFunction equals() {
    return new CompareIpFunction(SqlKind.EQUALS);
  }

  public static CompareIpFunction notEquals() {
    return new CompareIpFunction(SqlKind.NOT_EQUALS);
  }

  @Override
  public SqlUserDefinedFunction toUDF(String functionName, boolean isDeterministic) {
    SqlIdentifier udfIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        udfIdentifier,
        kind,
        getReturnTypeInference(),
        InferTypes.ANY_NULLABLE,
        getOperandMetadata(),
        getFunction()) {
      @Override
      public boolean isDeterministic() {
        return isDeterministic;
      }

      @Override
      public @Nullable SqlOperator reverse() {
        return switch (kind) {
          case LESS_THAN -> PPLBuiltinOperators.GREATER_IP;
          case GREATER_THAN -> PPLBuiltinOperators.LESS_IP;
          case LESS_THAN_OR_EQUAL -> PPLBuiltinOperators.GTE_IP;
          case GREATER_THAN_OR_EQUAL -> PPLBuiltinOperators.LTE_IP;
          case EQUALS -> PPLBuiltinOperators.EQUALS_IP;
          case NOT_EQUALS -> PPLBuiltinOperators.NOT_EQUALS_IP;
          default ->
              throw new IllegalArgumentException(
                  String.format(
                      Locale.ROOT, "CompareIpFunction is not supposed to be of kind: %s", kind));
        };
      }

      @Override
      public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
      }
    };
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        new SqlOperandTypeChecker() {
          @Override
          public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
              return false;
            }
            return OpenSearchTypeUtil.isIp(callBinding.getOperandType(0), true)
                && OpenSearchTypeUtil.isIp(callBinding.getOperandType(1), true);
          }

          @Override
          public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(2);
          }

          @Override
          public String getAllowedSignatures(SqlOperator op, String opName) {
            return String.format(Locale.ROOT, "%s(<IP>, <IP>)", opName);
          }
        });
  }

  public static class CompareImplementor implements NotNullImplementor {
    private final SqlKind compareType;

    public CompareImplementor(SqlKind compareType) {
      this.compareType = compareType;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression compareResult =
          Expressions.call(
              CompareImplementor.class,
              "compareTo",
              translatedOperands.get(0),
              translatedOperands.get(1));

      return evalCompareResult(compareResult, compareType);
    }

    private static Expression evalCompareResult(Expression compareResult, SqlKind compareType) {
      final ConstantExpression zero = Expressions.constant(0);
      return switch (compareType) {
        case EQUALS -> Expressions.equal(compareResult, zero);
        case NOT_EQUALS -> Expressions.notEqual(compareResult, zero);
        case LESS_THAN -> Expressions.lessThan(compareResult, zero);
        case LESS_THAN_OR_EQUAL -> Expressions.lessThanOrEqual(compareResult, zero);
        case GREATER_THAN -> Expressions.greaterThan(compareResult, zero);
        case GREATER_THAN_OR_EQUAL -> Expressions.greaterThanOrEqual(compareResult, zero);
        default ->
            throw new UnsupportedOperationException(
                String.format(Locale.ROOT, "Unsupported compare type: %s", compareType));
      };
    }

    public static int compareTo(Object obj1, Object obj2) {
      ExprIpValue v1 = toExprIpValue(obj1);
      ExprIpValue v2 = toExprIpValue(obj2);
      return v1.compare(v2);
    }

    private static ExprIpValue toExprIpValue(Object obj) {
      if (obj instanceof ExprIpValue) {
        return (ExprIpValue) obj;
      } else if (obj instanceof String) {
        return new ExprIpValue((String) obj);
      }
      throw new IllegalArgumentException("Invalid IP type: " + obj);
    }
  }
}
