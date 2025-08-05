/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.ImplementorUDF;
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
  private Supplier<SqlOperator> reverse;

  private CompareIpFunction(SqlKind kind) {
    super(new CompareImplementor(kind), NullPolicy.ANY);
    this.kind = kind;
    // Will be set later
    this.reverse = null;
  }

  @Override
  public SqlSyntax getSqlSyntax() {
    return SqlSyntax.BINARY;
  }

  @Override
  public SqlKind getKind() {
    return kind;
  }

  @Override
  public Supplier<SqlOperator> getReverse() {
    return reverse;
  }

  /**
   * Sets the reverse operator supplier for this comparison function.
   *
   * <p>This method is used to establish the reversed relationship between comparison operators
   * (e.g., "less than" and "greater than"). When the query optimizer normalizes expressions, it may
   * need to transform "b > a" to "a < b".
   *
   * <p>E.g. in the {@code hashCode} method of {@link org.apache.calcite.rex.RexNormalize}#L115, it
   * always converts <i>B [comparator] A</i> to <i>A [reverse_comparator] B</i> if the ordinal of
   * the reverse of the comparator is smaller.
   *
   * <p>IP comparison functions use this to inform the optimizer that ip_less_than is the reverse of
   * ip_greater_than, allowing for proper query normalization.
   *
   * @param supplier The supplier that provides the reverse SQL operator
   * @return This CompareIpFunction instance for method chaining
   */
  public CompareIpFunction withReverse(Supplier<SqlOperator> supplier) {
    this.reverse = supplier;
    return this;
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
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrapUDT(List.of(List.of(ExprCoreType.IP, ExprCoreType.IP)));
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
        default -> throw new UnsupportedOperationException(
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
