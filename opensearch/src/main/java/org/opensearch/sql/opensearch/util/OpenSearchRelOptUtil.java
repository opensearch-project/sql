/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil.RexFinder;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprType;

@UtilityClass
public class OpenSearchRelOptUtil {

  private static final EnumSet<ExprUDT> DATE_UDT_SET =
      EnumSet.of(ExprUDT.EXPR_TIMESTAMP, ExprUDT.EXPR_DATE, ExprUDT.EXPR_TIME);
  private static final List<DslTypeMappingRule> DSL_TYPE_MAPPING_RULES =
      Arrays.asList(
          // Not support time related UDT yet because derived field expects date to be Long but our UDT is actually a STRING
          new DslTypeMappingRule(
              t -> (t instanceof ExprSqlType) && ((ExprSqlType) t).getUdt() == ExprUDT.EXPR_IP,
              "ip"),
          new DslTypeMappingRule(t -> SqlTypeName.INT_TYPES.contains(t.getSqlTypeName()), "long"),
          // Float is not supported well in OpenSearch core. See reported bug: https://github.com/opensearch-project/OpenSearch/issues/19271
          // TODO: Support BigDecimal and other complex objects. A workaround is to wrap it in JSON object so that response can parse it
          new DslTypeMappingRule(
              t -> SqlTypeName.DOUBLE.equals(t.getSqlTypeName()), "double"),
          new DslTypeMappingRule(
              t -> SqlTypeName.BOOLEAN_TYPES.contains(t.getSqlTypeName()), "boolean"),
          new DslTypeMappingRule(
              t -> SqlTypeName.CHAR_TYPES.contains(t.getSqlTypeName()), "keyword"));

  /**
   * Given an input Calcite RexNode, find the single input field with equivalent collation
   * information. The function returns the pair of input field index and a flag to indicate whether
   * the ordering is flipped.
   *
   * @param expr Calcite expression node
   * @return Optional pair of input field index and flipped flag
   */
  public static Optional<Pair<Integer, Boolean>> getOrderEquivalentInputInfo(RexNode expr) {
    switch (expr.getKind()) {
      case INPUT_REF:
        RexInputRef inputRef = (RexInputRef) expr;
        return Optional.of(Pair.of(inputRef.getIndex(), false));
      case PLUS_PREFIX:
        return getOrderEquivalentInputInfo(((RexCall) expr).getOperands().get(0));
      case MINUS_PREFIX:
        return getOrderEquivalentInputInfo(((RexCall) expr).getOperands().get(0))
            .map(inputInfo -> Pair.of(inputInfo.getLeft(), !inputInfo.getRight()));
      case PLUS, MINUS:
        {
          RexNode operand0 = ((RexCall) expr).getOperands().get(0);
          RexNode operand1 = ((RexCall) expr).getOperands().get(1);

          boolean operand0Lit = operand0.isA(SqlKind.LITERAL);
          boolean operand1Lit = operand1.isA(SqlKind.LITERAL);

          if (operand0Lit == operand1Lit) {
            return Optional.empty();
          }

          RexNode variable = operand0Lit ? operand1 : operand0;
          boolean flipped = (expr.getKind() == SqlKind.MINUS) && operand0Lit;

          return getOrderEquivalentInputInfo(variable)
              .map(inputInfo -> Pair.of(inputInfo.getLeft(), flipped != inputInfo.getRight()));
        }
      case TIMES:
        {
          RexNode operand0 = ((RexCall) expr).getOperands().get(0);
          RexNode operand1 = ((RexCall) expr).getOperands().get(1);

          RexNode lit =
              operand0.isA(SqlKind.LITERAL)
                  ? operand0
                  : (operand1.isA(SqlKind.LITERAL) ? operand1 : null);
          RexNode variable = (lit == operand0) ? operand1 : operand0;

          if (lit == null) {
            return Optional.empty();
          }

          BigDecimal k = ((RexLiteral) lit).getValueAs(BigDecimal.class);
          if (k == null || k.signum() == 0) {
            return Optional.empty();
          }
          boolean flipped = k.signum() < 0;

          return getOrderEquivalentInputInfo(variable)
              .map(inputInfo -> Pair.of(inputInfo.getLeft(), flipped != inputInfo.getRight()));
        }
        // Ignore DIVIDE operator for now because it has too many precision issues
      case CAST, SAFE_CAST:
        {
          RexNode child = ((RexCall) expr).getOperands().get(0);
          if (!isOrderPreservingCast(child.getType(), expr.getType())) {
            return Optional.empty();
          }
          return getOrderEquivalentInputInfo(child);
        }
      default:
        return Optional.empty();
    }
  }

  public static String toDslType(RelDataType relDataType) {
    return DSL_TYPE_MAPPING_RULES.stream()
        .filter(rule -> rule.condition.test(relDataType))
        .findFirst()
        .map(DslTypeMappingRule::getResult)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Unsupported RelDataType for derived field: %s",
                        relDataType)));
  }

  private static class DslTypeMappingRule {
    @Getter private final Predicate<RelDataType> condition;
    @Getter private final String result;

    public DslTypeMappingRule(Predicate<RelDataType> condition, String result) {
      this.condition = condition;
      this.result = result;
    }
  }

  public static boolean findUDTType(RexNode node) {
    return node.accept(
        new RexVisitorImpl<>(true) {
          @Override
          public Boolean visitInputRef(RexInputRef inputRef) {
            return OpenSearchTypeFactory.isUserDefinedType(inputRef.getType());
          }

          @Override
          public Boolean visitLiteral(RexLiteral literal) {
            return OpenSearchTypeFactory.isUserDefinedType(literal.getType());
          }

          @Override
          public Boolean visitCall(RexCall call) {
            return OpenSearchTypeFactory.isUserDefinedType(call.getType());
          }
        });
  }

  private static boolean isOrderPreservingCast(RelDataType src, RelDataType dst) {
    final SqlTypeName srcType = src.getSqlTypeName();
    final SqlTypeName dstType = dst.getSqlTypeName();

    if (SqlTypeUtil.isIntType(src) && SqlTypeUtil.isApproximateNumeric(dst)) {
      int intBits =
          switch (srcType) {
            case TINYINT -> 8;
            case SMALLINT -> 16;
            case INTEGER -> 32;
            case BIGINT -> 64;
            default -> 0;
          };
      // Float and double can only handle exact number based on its significand precision
      int floatBits =
          switch (dstType) {
            case FLOAT -> 24;
            case DOUBLE -> 53;
            default -> 0;
          };
      return intBits > 0 && floatBits > 0 && intBits <= floatBits;
    }

    if (SqlTypeUtil.isExactNumeric(src) && SqlTypeUtil.isExactNumeric(dst)) {
      int srcPrec = src.getPrecision();
      int dstPrec = dst.getPrecision();
      return dstPrec >= srcPrec;
    }

    if (SqlTypeUtil.isCharacter(src) && SqlTypeUtil.isCharacter(dst)) {
      int srcLength = src.getPrecision();
      int dstLength = dst.getPrecision();
      return dstLength >= srcLength || dstLength == RelDataType.PRECISION_NOT_SPECIFIED;
    }

    if (srcType == SqlTypeName.DATE
        && (dstType == SqlTypeName.TIMESTAMP
            || dstType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
      return true;
    }

    if (srcType == SqlTypeName.TIME
        && (dstType == SqlTypeName.TIMESTAMP
            || dstType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
      return true;
    }

    if (srcType == dstType) {
      return dst.getPrecision() >= src.getPrecision() && dst.getScale() >= src.getScale();
    }

    return false;
  }
}
