/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class ExtendedRexBuilder extends RexBuilder {

  public ExtendedRexBuilder(RexBuilder rexBuilder) {
    super(rexBuilder.getTypeFactory());
  }

  public RexNode coalesce(RexNode... nodes) {
    return this.makeCall(SqlStdOperatorTable.COALESCE, nodes);
  }

  public RexNode equals(RexNode n1, RexNode n2) {
    return this.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }

  public RexNode and(RexNode left, RexNode right) {
    final RelDataType booleanType = this.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    return this.makeCall(booleanType, SqlStdOperatorTable.AND, List.of(left, right));
  }

  public RelDataType commonType(RexNode... nodes) {
    return this.getTypeFactory()
        .leastRestrictive(Arrays.stream(nodes).map(RexNode::getType).toList());
  }

  public SqlIntervalQualifier createIntervalUntil(SpanUnit unit) {
    TimeUnit timeUnit;
    switch (unit) {
      case MICROSECOND:
      case US:
        timeUnit = TimeUnit.MICROSECOND;
        break;
      case MILLISECOND:
      case MS:
        timeUnit = TimeUnit.MILLISECOND;
        break;
      case SECONDS:
      case SECOND:
      case SECS:
      case SEC:
      case S:
        timeUnit = TimeUnit.SECOND;
        break;
      case MINUTES:
      case MINUTE:
      case MINS:
      case MIN:
      case m:
        timeUnit = TimeUnit.MINUTE;
        break;
      case HOURS:
      case HOUR:
      case HRS:
      case HR:
      case H:
        timeUnit = TimeUnit.HOUR;
        break;
      case DAYS:
      case DAY:
      case D:
        timeUnit = TimeUnit.DAY;
        break;
      case WEEKS:
      case WEEK:
      case W:
        timeUnit = TimeUnit.WEEK;
        break;
      case MONTHS:
      case MONTH:
      case MON:
      case M:
        timeUnit = TimeUnit.MONTH;
        break;
      case QUARTERS:
      case QUARTER:
      case QTRS:
      case QTR:
      case Q:
        timeUnit = TimeUnit.QUARTER;
        break;
      case YEARS:
      case YEAR:
      case Y:
        timeUnit = TimeUnit.YEAR;
        break;
      default:
        timeUnit = TimeUnit.EPOCH;
    }
    return new SqlIntervalQualifier(timeUnit, timeUnit, SqlParserPos.ZERO);
  }

  /**
   * Casts the given expression to the requested SQL type, applying framework-specific conversions
   * and special-case rules for booleans, user-defined types, and number-to-character casts.
   *
   * <p>Behavior highlights:
   * - Converts character literal "1"/"0" to boolean `true`/`false`; converts exact numeric to boolean
   *   by comparing against zero.
   * - For user-defined types (UDT), delegates conversion to corresponding PPL builtin operators
   *   (DATE, TIME, TIMESTAMP, IP) when applicable; preserves IP values and converts STRING to IP.
   * - Converts floating point/decimal numbers to character using the `NUMBER_TO_STRING` operator.
   *
   * @param pos the parser position associated with this cast
   * @param type the target relational data type to cast to
   * @param exp the expression to be cast
   * @param matchNullability whether the resulting type's nullability should match the target
   * @param safe whether the cast should be performed in a "safe" mode (no runtime errors)
   * @param format optional literal specifying a target format (may be null)
   * @return a RexNode representing the cast or an equivalent expression implementing the conversion
   * @throws ExpressionEvaluationException if converting to IP from an unsupported argument type
   * @throws SemanticCheckException if casting to a user-defined type is unsupported for the argument
   */
  @Override
  public RexNode makeCast(
      SqlParserPos pos,
      RelDataType type,
      RexNode exp,
      boolean matchNullability,
      boolean safe,
      RexLiteral format) {
    final SqlTypeName sqlType = type.getSqlTypeName();
    RelDataType sourceType = exp.getType();
    // Calcite bug which doesn't consider to cast literal to boolean
    if (exp instanceof RexLiteral && sqlType == SqlTypeName.BOOLEAN) {
      if (exp.equals(makeLiteral("1", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(true, type);
      } else if (exp.equals(makeLiteral("0", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(false, type);
      } else if (SqlTypeUtil.isExactNumeric(sourceType)) {
        return makeCall(
            type,
            SqlStdOperatorTable.NOT_EQUALS,
            ImmutableList.of(exp, makeZeroLiteral(sourceType)));
        // TODO https://github.com/opensearch-project/sql/issues/3443
        // Current, we align the behaviour of Spark and Postgres, to align with OpenSearch V2,
        // enable following commented codes.
        //      } else {
        //        return makeCall(type,
        //            SqlStdOperatorTable.NOT_EQUALS,
        //            ImmutableList.of(exp, makeZeroLiteral(sourceType)));
      }
    } else if (OpenSearchTypeUtil.isUserDefinedType(type)) {
      if (RexLiteral.isNullLiteral(exp)) {
        return super.makeCast(pos, type, exp, matchNullability, safe, format);
      }
      var udt = ((AbstractExprRelDataType<?>) type).getUdt();
      var argExprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(sourceType);
      return switch (udt) {
        case EXPR_DATE -> makeCall(type, PPLBuiltinOperators.DATE, List.of(exp));
        case EXPR_TIME -> makeCall(type, PPLBuiltinOperators.TIME, List.of(exp));
        case EXPR_TIMESTAMP -> makeCall(type, PPLBuiltinOperators.TIMESTAMP, List.of(exp));
        case EXPR_IP -> {
          if (argExprType == ExprCoreType.IP) {
            yield exp;
          } else if (argExprType == ExprCoreType.STRING) {
            yield makeCall(type, PPLBuiltinOperators.IP, List.of(exp));
          }
          // Throwing error inside implementation will be suppressed by Calcite, thus
          // throwing 500 error. Therefore, we throw error here to ensure the error
          // information is displayed properly.
          throw new ExpressionEvaluationException(
              String.format(
                  Locale.ROOT,
                  "Cannot convert %s to IP, only STRING and IP types are supported",
                  argExprType));
        }
        default ->
            throw new SemanticCheckException(
                String.format(Locale.ROOT, "Cannot cast from %s to %s", argExprType, udt.name()));
      };
    }
    // Use a custom operator when casting floating point or decimal number to a character type.
    // This patch is necessary because in Calcite, 0.0F is cast to 0E0, decimal 0.x to x
    else if ((SqlTypeUtil.isApproximateNumeric(sourceType) || SqlTypeUtil.isDecimal(sourceType))
        && SqlTypeUtil.isCharacter(type)) {
      // NUMBER_TO_STRING uses java's built-in method to get the string representation of a number
      return makeCall(type, PPLBuiltinOperators.NUMBER_TO_STRING, List.of(exp));
    }
    return super.makeCast(pos, type, exp, matchNullability, safe, format);
  }

  /**
   * Infers the result type for a call to the given operator.
   *
   * <p>When the operator is a binary arithmetic and one operand is numeric while the other is a
   * character type, returns the numeric operand's type. For all other cases, delegates to the
   * superclass implementation.
   *
   * @param op the operator being called
   * @param exprs the actual operand expressions
   * @return the inferred relational data type for the operator call
   */
  @Override
  public RelDataType deriveReturnType(SqlOperator op, List<? extends RexNode> exprs) {
    if (op.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC) && exprs.size() == 2) {
      final RelDataType type1 = exprs.get(0).getType();
      final RelDataType type2 = exprs.get(1).getType();
      if (SqlTypeUtil.isNumeric(type1) && OpenSearchTypeUtil.isCharacter(type2)) {
        return type1;
      } else if (OpenSearchTypeUtil.isCharacter(type1) && SqlTypeUtil.isNumeric(type2)) {
        return type2;
      }
    }
    return super.deriveReturnType(op, exprs);
  }
}