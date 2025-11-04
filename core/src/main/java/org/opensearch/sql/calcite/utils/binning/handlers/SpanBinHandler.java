/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import static org.apache.calcite.sql.SqlKind.LITERAL;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.*;

/** Handler for span-based binning operations. */
public class SpanBinHandler implements BinHandler {

  private final NumericSpanHelper numericHelper = new NumericSpanHelper();
  private final LogSpanHelper logHelper = new LogSpanHelper();
  private final TimeSpanHelper timeHelper = new TimeSpanHelper();

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    SpanBin spanBin = (SpanBin) node;

    // Create validated binnable field (validates that field is numeric or time-based)
    String fieldName = BinFieldValidator.extractFieldName(node);
    BinnableField field = new BinnableField(fieldExpr, fieldExpr.getType(), fieldName);

    // Handle time-based fields
    if (field.isTimeBased()) {
      return handleTimeBasedSpan(spanBin, fieldExpr, context, visitor);
    }

    // Handle numeric/log spans
    return handleNumericOrLogSpan(spanBin, fieldExpr, context, visitor);
  }

  private RexNode handleTimeBasedSpan(
      SpanBin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    if (node.getSpan() instanceof Literal) {
      Literal spanLiteral = (Literal) node.getSpan();
      String spanStr = spanLiteral.getValue().toString();

      // Process aligntime if present
      RexNode alignTimeValue = processAligntime(node, fieldExpr, context, visitor);

      return timeHelper.createTimeSpanExpression(spanStr, fieldExpr, alignTimeValue, context);
    } else {
      RexNode spanValue = visitor.analyze(node.getSpan(), context);
      if (!spanValue.isA(LITERAL)) {
        throw new IllegalArgumentException("Span must be a literal value for time binning");
      }
      String spanStr = ((RexLiteral) spanValue).getValue().toString();

      RexNode alignTimeValue = processAligntime(node, fieldExpr, context, visitor);
      return timeHelper.createTimeSpanExpression(spanStr, fieldExpr, alignTimeValue, context);
    }
  }

  private RexNode handleNumericOrLogSpan(
      SpanBin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    // Field is already validated by createExpression - must be numeric since we're in this branch
    RexNode spanValue = visitor.analyze(node.getSpan(), context);

    if (!spanValue.isA(LITERAL)) {
      throw new IllegalArgumentException("Span must be a literal value");
    }

    Object spanRawValue = ((RexLiteral) spanValue).getValue();

    if (spanRawValue instanceof org.apache.calcite.util.NlsString) {
      String spanStr = ((org.apache.calcite.util.NlsString) spanRawValue).getValue();
      SpanInfo spanInfo = SpanParser.parse(spanStr);

      if (spanInfo.getType() == SpanType.LOG) {
        return logHelper.createLogSpanExpression(fieldExpr, spanInfo, context);
      } else {
        return numericHelper.createNumericSpanExpression(
            fieldExpr, (int) spanInfo.getValue(), context);
      }
    } else if (spanRawValue instanceof Number) {
      Number spanNumber = (Number) spanRawValue;
      if (spanNumber.doubleValue() == Math.floor(spanNumber.doubleValue())) {
        return numericHelper.createNumericSpanExpression(fieldExpr, spanNumber.intValue(), context);
      } else {
        return numericHelper.createNumericSpanExpression(
            fieldExpr, spanNumber.doubleValue(), context);
      }
    } else {
      throw new IllegalArgumentException(
          "Span must be either a number or a string, got: "
              + spanRawValue.getClass().getSimpleName());
    }
  }

  private RexNode processAligntime(
      SpanBin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    if (node.getAligntime() == null) {
      return null;
    }

    if (node.getAligntime() instanceof Literal) {
      Literal aligntimeLiteral = (Literal) node.getAligntime();
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
          return visitor.analyze(node.getAligntime(), context);
        }
      }
    } else {
      return visitor.analyze(node.getAligntime(), context);
    }
  }
}
