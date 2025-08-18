/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.*;

/**
 * Simplified facade for bin command operations in Calcite. Delegates to specialized handlers for
 * different bin types.
 */
public class BinUtils {

  /**
   * Extracts the field name from a Bin node.
   *
   * @deprecated Use FieldValidator.extractFieldName instead
   */
  @Deprecated
  public static String extractFieldName(Bin node) {
    return FieldValidator.extractFieldName(node);
  }

  /**
   * Processes the aligntime parameter and returns the corresponding RexNode.
   *
   * @deprecated Use SpanBinHandler directly
   */
  @Deprecated
  public static RexNode processAligntimeParameter(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor rexVisitor) {

    if (!(node instanceof SpanBin)) {
      return null;
    }

    SpanBin spanBin = (SpanBin) node;
    if (spanBin.getAligntime() == null) {
      return null;
    }

    if (!FieldValidator.isTimeBasedField(fieldExpr.getType())) {
      return null;
    }

    FieldValidator.validateFieldExists(FieldValidator.extractFieldName(node), context);

    if (spanBin.getAligntime() instanceof Literal) {
      Literal aligntimeLiteral = (Literal) spanBin.getAligntime();
      String aligntimeStr =
          aligntimeLiteral.getValue().toString().replace("\"", "").replace("'", "").trim();

      if ("earliest".equals(aligntimeStr)) {
        return context.relBuilder.min(fieldExpr).over().toRex();
      } else if ("latest".equals(aligntimeStr)) {
        return context.relBuilder.max(fieldExpr).over().toRex();
      } else if (aligntimeStr.startsWith("@d")) {
        return context.relBuilder.literal(
            BinConstants.ALIGNTIME_TIME_MODIFIER_PREFIX + aligntimeStr);
      } else {
        try {
          long epochValue = Long.parseLong(aligntimeStr);
          return context.relBuilder.literal(BinConstants.ALIGNTIME_EPOCH_PREFIX + epochValue);
        } catch (NumberFormatException e) {
          return rexVisitor.analyze(spanBin.getAligntime(), context);
        }
      }
    } else {
      return rexVisitor.analyze(spanBin.getAligntime(), context);
    }
  }

  /** Creates the appropriate bin expression that transforms field values to range strings. */
  public static RexNode createBinExpression(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      CalciteRexNodeVisitor rexVisitor) {

    BinHandler handler = BinHandlerFactory.getHandler(node);
    return handler.createExpression(node, fieldExpr, context, rexVisitor);
  }

  /** Determines if the bin command uses window functions. */
  public static boolean usesWindowFunctions(Bin node) {
    BinHandler handler = BinHandlerFactory.getHandler(node);
    return handler.usesWindowFunctions();
  }

  /**
   * Validates that the specified field exists in the dataset.
   *
   * @deprecated Use FieldValidator.validateFieldExists instead
   */
  @Deprecated
  public static void validateFieldExists(String fieldName, CalcitePlanContext context) {
    FieldValidator.validateFieldExists(fieldName, context);
  }

  /**
   * Checks if the field type is time-based.
   *
   * @deprecated Use FieldValidator.isTimeBasedField instead
   */
  @Deprecated
  public static boolean isTimeBasedField(org.apache.calcite.rel.type.RelDataType fieldType) {
    return FieldValidator.isTimeBasedField(fieldType);
  }

  // Legacy methods for backward compatibility - delegate to SpanParser

  /**
   * @deprecated Use SpanParser.parse instead
   */
  @Deprecated
  public static SpanInfo parseSpanString(String spanStr) {
    return SpanParser.parse(spanStr);
  }

  /**
   * @deprecated Use SpanParser.extractTimeUnit instead
   */
  @Deprecated
  public static String extractTimeUnit(String spanStr) {
    return SpanParser.extractTimeUnit(spanStr);
  }
}
