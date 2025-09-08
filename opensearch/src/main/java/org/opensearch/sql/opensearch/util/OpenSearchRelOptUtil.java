/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.math.BigDecimal;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.lang3.tuple.Pair;

@UtilityClass
public class OpenSearchRelOptUtil {

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
      case PLUS:
      case MINUS:
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
      case CAST:
      case SAFE_CAST:
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

  private static boolean isOrderPreservingCast(RelDataType src, RelDataType dst) {
    final SqlTypeName srcType = src.getSqlTypeName();
    final SqlTypeName dstType = dst.getSqlTypeName();

    if (SqlTypeUtil.isIntType(src) && SqlTypeUtil.isApproximateNumeric(dst)) {
      int intBits;
      switch (srcType) {
        case TINYINT:
          intBits = 8;
          break;
        case SMALLINT:
          intBits = 16;
          break;
        case INTEGER:
          intBits = 32;
          break;
        case BIGINT:
          intBits = 64;
          break;
        default:
          intBits = 0;
          break;
      };
      // Float and double can only handle exact number based on its significand precision
      int floatBits;
      switch (dstType) {
        case FLOAT:
          floatBits = 24;
          break;
        case DOUBLE:
          floatBits = 53;
          break;
        default:
          floatBits = 0;
          break;
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
