/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.SqlKind.LITERAL;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Utility class for handling bin command operations in Calcite. Contains helper methods for
 * processing bin parameters and creating bin expressions.
 */
public class BinUtils {

  /** Extracts the field name from a Bin node. */
  public static String extractFieldName(Bin node) {
    if (node.getField() instanceof Field) {
      Field field = (Field) node.getField();
      return field.getField().toString();
    } else {
      return node.getField().toString();
    }
  }

  /** Determines the alias name for the binned field. */
  public static String determineAliasName(Bin node, String fieldName) {
    return node.getAlias() != null ? node.getAlias() : fieldName + "_bin";
  }

  /** Processes the aligntime parameter and returns the corresponding RexNode. */
  public static RexNode processAligntimeParameter(
      Bin node,
      RexNode fieldExpr,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {
    if (node.getAligntime() == null) {
      return null;
    }

    RelDataType fieldType = fieldExpr.getType();
    // Aligntime is only valid for time-based fields
    if (!isTimeBasedField(fieldType)) {
      return null;
    }

    if (node.getAligntime() instanceof Literal) {
      Literal aligntimeLiteral = (Literal) node.getAligntime();
      String aligntimeStr = aligntimeLiteral.getValue().toString();

      if ("earliest".equals(aligntimeStr)) {
        // For earliest, we align to epoch 0, but since subtracting and adding 0
        // is a no-op, we can just use null to indicate no alignment needed
        return null;
      } else if ("latest".equals(aligntimeStr)) {
        // Calculate maximum value using aggregate
        // For now, we'll use a placeholder - in production, this would require
        // a subquery or window function to get the actual max value
        return context.relBuilder.literal(System.currentTimeMillis());
      } else {
        // Parse as a time value
        return rexVisitor.analyze(node.getAligntime(), context);
      }
    } else {
      // It's a time expression
      return rexVisitor.analyze(node.getAligntime(), context);
    }
  }

  /** Creates the appropriate bin expression based on the bin parameters. */
  public static RexNode createBinExpression(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {
    if (node.getSpan() != null) {
      return createSpanBasedBinning(node, fieldExpr, alignTimeValue, context, rexVisitor);
    } else if (node.getMinspan() != null) {
      return createMinspanBasedBinning(node, fieldExpr, alignTimeValue, context, rexVisitor);
    } else if (node.getBins() != null) {
      return createBinsBasedBinning(node, fieldExpr, context);
    } else {
      return createDefaultBinning(fieldExpr, context);
    }
  }

  /** Creates span-based binning expression. */
  public static RexNode createSpanBasedBinning(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {
    RexNode spanValue = rexVisitor.analyze(node.getSpan(), context);
    RelDataType fieldType = fieldExpr.getType();
    RexNode unitNode = determineUnitForField(fieldType, context, "ms");
    boolean isDecimalSpan = isDecimalValue(spanValue);

    if (shouldUseSpanFunction(fieldType, unitNode, isDecimalSpan)) {
      return createSpanFunctionBinning(
          fieldExpr, spanValue, unitNode, alignTimeValue, fieldType, isDecimalSpan, context);
    } else {
      return createDirectBinning(fieldExpr, spanValue, alignTimeValue, fieldType, context);
    }
  }

  /** Creates minspan-based binning expression. */
  public static RexNode createMinspanBasedBinning(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {
    RexNode minspanValue = rexVisitor.analyze(node.getMinspan(), context);
    RelDataType fieldType = fieldExpr.getType();
    RexNode spanValue = minspanValue; // Default implementation: use minspan as the span value
    RexNode unitNode = determineUnitForField(fieldType, context, "");
    boolean isDecimalSpan = isDecimalValue(spanValue);

    if (shouldUseSpanFunction(fieldType, unitNode, isDecimalSpan)) {
      return createSpanFunctionBinning(
          fieldExpr, spanValue, unitNode, alignTimeValue, fieldType, isDecimalSpan, context);
    } else {
      return createDirectBinning(fieldExpr, spanValue, alignTimeValue, fieldType, context);
    }
  }

  /** Creates bins-based binning expression. */
  public static RexNode createBinsBasedBinning(
      Bin node, RexNode fieldExpr, CalcitePlanContext context) {
    Integer numBins = node.getBins();
    // Use a default range - this should be improved to calculate actual data range
    RexNode minValue = context.relBuilder.literal(0.0);
    RexNode range = context.relBuilder.literal(1000.0);
    RexNode binSize =
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, range, context.relBuilder.literal(numBins));

    RelDataType fieldType = fieldExpr.getType();
    RexNode workingFieldExpr = fieldExpr;

    if (fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      workingFieldExpr = context.relBuilder.cast(fieldExpr, SqlTypeName.BIGINT);
    }

    // bin = floor((field - min) / bin_size) * bin_size + min
    RexNode shifted =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, workingFieldExpr, minValue);
    RexNode divided =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.DIVIDE, shifted, binSize);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode multiplied = context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, binSize);
    return context.relBuilder.call(SqlStdOperatorTable.PLUS, multiplied, minValue);
  }

  /** Creates default binning expression. */
  public static RexNode createDefaultBinning(RexNode fieldExpr, CalcitePlanContext context) {
    RelDataType fieldType = fieldExpr.getType();
    RexNode defaultSpan = context.relBuilder.literal(1);
    RexNode workingFieldExpr = fieldExpr;

    if (fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      workingFieldExpr = context.relBuilder.cast(fieldExpr, SqlTypeName.BIGINT);
    }

    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, workingFieldExpr, defaultSpan);
    return context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
  }

  /** Determines the unit parameter for the given field type. */
  public static RexNode determineUnitForField(
      RelDataType fieldType, CalcitePlanContext context, String defaultTimeUnit) {
    if (fieldType.getSqlTypeName() == SqlTypeName.BIGINT
        || fieldType.getSqlTypeName() == SqlTypeName.INTEGER
        || fieldType.getSqlTypeName() == SqlTypeName.SMALLINT
        || fieldType.getSqlTypeName() == SqlTypeName.TINYINT
        || fieldType.getSqlTypeName() == SqlTypeName.DOUBLE
        || fieldType.getSqlTypeName() == SqlTypeName.FLOAT
        || fieldType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return context.relBuilder.literal(null);
    } else {
      return context.relBuilder.literal(defaultTimeUnit);
    }
  }

  /** Checks if the given value is a decimal number. */
  public static boolean isDecimalValue(RexNode value) {
    if (value.isA(LITERAL) && ((RexLiteral) value).getValue() != null) {
      Object val = ((RexLiteral) value).getValue();
      if (val instanceof Number) {
        double doubleVal = ((Number) val).doubleValue();
        return (doubleVal != Math.floor(doubleVal));
      }
    }
    return false;
  }

  /** Determines whether to use the SPAN function or direct mathematical operations. */
  public static boolean shouldUseSpanFunction(
      RelDataType fieldType, RexNode unitNode, boolean isDecimalSpan) {
    return fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || fieldType.getSqlTypeName() == SqlTypeName.DATE
        || (unitNode.isA(LITERAL)
            && ((RexLiteral) unitNode).getValue() != null
            && !((RexLiteral) unitNode).getValue().toString().isEmpty());
  }

  /** Creates binning expression using the SPAN function. */
  public static RexNode createSpanFunctionBinning(
      RexNode fieldExpr,
      RexNode spanValue,
      RexNode unitNode,
      RexNode alignTimeValue,
      RelDataType fieldType,
      boolean isDecimalSpan,
      CalcitePlanContext context) {
    // For integer-like fields with null unit, ensure span is also INTEGER
    if (isIntegerLikeField(fieldType)
        && unitNode.isA(LITERAL)
        && ((RexLiteral) unitNode).getValue() == null
        && !isDecimalSpan) {
      spanValue = context.relBuilder.cast(spanValue, SqlTypeName.INTEGER);
    }

    if (alignTimeValue != null) {
      // For time alignment, adjust the field value before binning
      RexNode adjustedField =
          context.relBuilder.call(SqlStdOperatorTable.MINUS, fieldExpr, alignTimeValue);
      RexNode binExpression =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.SPAN, adjustedField, spanValue, unitNode);
      return context.relBuilder.call(SqlStdOperatorTable.PLUS, binExpression, alignTimeValue);
    } else {
      return PPLFuncImpTable.INSTANCE.resolve(
          context.rexBuilder, BuiltinFunctionName.SPAN, fieldExpr, spanValue, unitNode);
    }
  }

  /** Creates binning expression using direct mathematical operations. */
  public static RexNode createDirectBinning(
      RexNode fieldExpr,
      RexNode spanValue,
      RexNode alignTimeValue,
      RelDataType fieldType,
      CalcitePlanContext context) {
    RexNode workingFieldExpr = fieldExpr;

    if (alignTimeValue != null && shouldApplyAlignment(fieldType)) {
      // For time alignment: aligned_bin = floor((field - aligntime) / span) * span + aligntime
      RexNode adjusted =
          context.relBuilder.call(SqlStdOperatorTable.MINUS, workingFieldExpr, alignTimeValue);
      RexNode divided = context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjusted, spanValue);
      RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      RexNode multiplied =
          context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, spanValue);
      return context.relBuilder.call(SqlStdOperatorTable.PLUS, multiplied, alignTimeValue);
    } else {
      // Standard binning: bin = FLOOR(field / span) * span
      RexNode divided =
          context.relBuilder.call(SqlStdOperatorTable.DIVIDE, workingFieldExpr, spanValue);
      RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      return context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, spanValue);
    }
  }

  /** Checks if the field type is time-based. */
  public static boolean isTimeBasedField(RelDataType fieldType) {
    return fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || fieldType.getSqlTypeName() == SqlTypeName.BIGINT
        || fieldType.getSqlTypeName() == SqlTypeName.DATE;
  }

  /** Checks if the field type is integer-like. */
  public static boolean isIntegerLikeField(RelDataType fieldType) {
    return fieldType.getSqlTypeName() == SqlTypeName.BIGINT
        || fieldType.getSqlTypeName() == SqlTypeName.INTEGER
        || fieldType.getSqlTypeName() == SqlTypeName.SMALLINT
        || fieldType.getSqlTypeName() == SqlTypeName.TINYINT;
  }

  /** Determines if alignment should be applied for the given field type. */
  public static boolean shouldApplyAlignment(RelDataType fieldType) {
    return fieldType.getSqlTypeName() == SqlTypeName.BIGINT
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }
}
