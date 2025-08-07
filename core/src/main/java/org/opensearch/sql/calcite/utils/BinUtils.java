/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.SqlKind.LITERAL;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Utility class for handling bin command operations in Calcite. Contains helper methods for
 * processing bin parameters and creating bin expressions.
 */
public class BinUtils {

  // Constants
  private static final String TIMESTAMP_FIELD = "@timestamp";
  private static final String DASH_SEPARATOR = "-";
  private static final String OTHER_CATEGORY = "Other";
  private static final String INVALID_CATEGORY = "Invalid";
  private static final int DEFAULT_BINS = 100;
  private static final int MIN_BINS = 2;
  private static final int MAX_BINS = 50000;
  private static final double[] SPL_NICE_WIDTHS = {
    0.001,
    0.01,
    0.1,
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0
  };

  // Time unit constants
  private static final long MILLISECONDS_PER_SECOND = 1000L;
  private static final long MILLISECONDS_PER_MINUTE = 60 * MILLISECONDS_PER_SECOND;
  private static final long MILLISECONDS_PER_HOUR = 60 * MILLISECONDS_PER_MINUTE;

  /** Extracts the field name from a Bin node. */
  public static String extractFieldName(Bin node) {
    if (node.getField() instanceof Field) {
      Field field = (Field) node.getField();
      return field.getField().toString();
    } else {
      return node.getField().toString();
    }
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
    String fieldName = extractFieldName(node);

    if (!shouldApplyTimeBinning(fieldName, fieldType)) {
      return null;
    }

    validateTimestampFieldExists(fieldName, context);

    if (node.getAligntime() instanceof Literal) {
      Literal aligntimeLiteral = (Literal) node.getAligntime();
      String aligntimeStr = aligntimeLiteral.getValue().toString();

      if ("earliest".equals(aligntimeStr)) {
        return context.relBuilder.min(fieldExpr).over().toRex();
      } else if ("latest".equals(aligntimeStr)) {
        return context.relBuilder.max(fieldExpr).over().toRex();
      } else {
        RexNode alignTimeValue = rexVisitor.analyze(node.getAligntime(), context);
        if (alignTimeValue instanceof org.apache.calcite.rex.RexLiteral) {
          Object value = ((org.apache.calcite.rex.RexLiteral) alignTimeValue).getValue();
          if (value instanceof Number) {
            return context.relBuilder.literal(((Number) value).longValue());
          }
        }
        return alignTimeValue;
      }
    } else {
      // It's a time expression
      return rexVisitor.analyze(node.getAligntime(), context);
    }
  }

  /**
   * Creates the appropriate bin expression that transforms field values to range strings. This is
   * the core SPL-compatible implementation that generates expressions like: CASE WHEN field >= 30
   * AND field < 35 THEN '30-35' WHEN field >= 35 AND field < 40 THEN '35-40' ELSE 'Other' END
   */
  public static RexNode createBinExpression(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {
    if (node.getSpan() != null) {
      return createSpanBasedRangeStrings(node, fieldExpr, alignTimeValue, context, rexVisitor);
    } else if (node.getMinspan() != null) {
      return createMinspanBasedRangeStrings(node, fieldExpr, context, rexVisitor);
    } else if (node.getBins() != null) {
      return createBinsBasedRangeStrings(node, fieldExpr, context);
    } else if (node.getStart() != null || node.getEnd() != null) {
      return createStartEndRangeStrings(node, fieldExpr, context, rexVisitor);
    } else {
      return createDefaultRangeStrings(node, fieldExpr, context);
    }
  }

  /**
   * Creates span-based range strings like "30-35", "35-40". This replaces the old numeric binning
   * with SPL-compatible string ranges.
   */
  public static RexNode createSpanBasedRangeStrings(
      Bin node,
      RexNode fieldExpr,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {

    String fieldName = extractFieldName(node);
    RelDataType fieldType = fieldExpr.getType();

    if (shouldApplyTimeBinning(fieldName, fieldType)) {
      validateTimestampFieldExists(fieldName, context);
      if (node.getSpan() instanceof org.opensearch.sql.ast.expression.Literal) {
        org.opensearch.sql.ast.expression.Literal spanLiteral =
            (org.opensearch.sql.ast.expression.Literal) node.getSpan();
        String spanStr = spanLiteral.getValue().toString();

        return createBinSpanExpressionFromString(spanStr, fieldExpr, alignTimeValue, context);
      } else {
        RexNode spanValue = rexVisitor.analyze(node.getSpan(), context);
        if (!spanValue.isA(LITERAL)) {
          throw new IllegalArgumentException("Span must be a literal value for time binning");
        }
        String spanStr = ((RexLiteral) spanValue).getValue().toString();
        return createBinSpanExpressionFromString(spanStr, fieldExpr, alignTimeValue, context);
      }
    }

    RexNode spanValue = rexVisitor.analyze(node.getSpan(), context);

    if (!spanValue.isA(LITERAL)) {
      throw new IllegalArgumentException(
          "Span must be a literal value for range string generation");
    }

    Object spanRawValue = ((RexLiteral) spanValue).getValue();

    if (spanRawValue instanceof org.apache.calcite.util.NlsString) {
      String spanStr = ((org.apache.calcite.util.NlsString) spanRawValue).getValue();
      return createSpanBasedExpression(spanStr, fieldExpr, context);
    } else if (spanRawValue instanceof Number) {
      int span = ((Number) spanRawValue).intValue();
      return createRangeCaseExpression(
          fieldExpr, span, alignTimeValue, context, null, null, rexVisitor);
    } else {
      throw new IllegalArgumentException(
          "Span must be either a number or a string (for log-based spans), got: "
              + spanRawValue.getClass().getSimpleName());
    }
  }

  /**
   * Determines if the bin command uses window functions that cause script size issues. Window
   * functions (MIN() OVER(), MAX() OVER()) are used by bins, minspan, start/end, and default
   * parameters to calculate data ranges dynamically.
   */
  public static boolean usesWindowFunctions(Bin node) {
    return node.getBins() != null
        || node.getMinspan() != null
        || node.getStart() != null
        || node.getEnd() != null
        || (node.getSpan() == null); // default behavior also uses window functions
  }

  /** Creates minspan-based range strings using SPL's magnitude-based minspan algorithm. */
  public static RexNode createMinspanBasedRangeStrings(
      Bin node,
      RexNode fieldExpr,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {

    RexNode minspanValue = rexVisitor.analyze(node.getMinspan(), context);

    if (!minspanValue.isA(LITERAL)) {
      throw new IllegalArgumentException(
          "Minspan must be a literal value for range string generation");
    }

    Number minspanNum = (Number) ((RexLiteral) minspanValue).getValue();
    double minspan = minspanNum.doubleValue();

    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();

    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    double log10Minspan = Math.log10(minspan);
    double ceilLog = Math.ceil(log10Minspan);
    double minspanWidth = Math.pow(10, ceilLog);

    RexNode log10Range = context.relBuilder.call(SqlStdOperatorTable.LOG10, dataRange);
    RexNode floorLog = context.relBuilder.call(SqlStdOperatorTable.FLOOR, log10Range);
    RexNode defaultWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.POWER, context.relBuilder.literal(10.0), floorLog);

    RexNode useDefault =
        context.relBuilder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            defaultWidth,
            context.relBuilder.literal(minspan));

    RexNode selectedWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            useDefault,
            defaultWidth,
            context.relBuilder.literal(minspanWidth));

    RexNode firstBinStart =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY,
            context.relBuilder.call(
                SqlStdOperatorTable.FLOOR,
                context.relBuilder.call(SqlStdOperatorTable.DIVIDE, minValue, selectedWidth)),
            selectedWidth);

    RexNode binValue = calculateBinValue(fieldExpr, selectedWidth, firstBinStart, context);

    RexNode binEnd = context.relBuilder.call(SqlStdOperatorTable.PLUS, binValue, selectedWidth);
    return createRangeString(binValue, binEnd, selectedWidth, context);
  }

  /** Creates bins-based range strings using SPL's exact "nice number" algorithm. */
  public static RexNode createBinsBasedRangeStrings(
      Bin node, RexNode fieldExpr, CalcitePlanContext context) {

    Integer requestedBins = node.getBins();
    if (requestedBins == null) {
      requestedBins = DEFAULT_BINS;
    }

    // Validate bins constraint
    if (requestedBins < MIN_BINS) {
      throw new IllegalArgumentException(
          "The bins parameter must be at least " + MIN_BINS + ", got: " + requestedBins);
    }
    if (requestedBins > MAX_BINS) {
      throw new IllegalArgumentException(
          "The bins parameter must not exceed " + MAX_BINS + ", got: " + requestedBins);
    }

    return createFallbackBinningExpression(fieldExpr, requestedBins, context);
  }

  /** Creates range strings when only start/end parameters are specified (without bins). */
  public static RexNode createStartEndRangeStrings(
      Bin node,
      RexNode fieldExpr,
      CalcitePlanContext context,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {

    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();

    RexNode effectiveMin = minValue; // default to data min
    RexNode effectiveMax = maxValue; // default to data max

    if (node.getStart() != null) {
      RexNode startValue = rexVisitor.analyze(node.getStart(), context);
      effectiveMin =
          context.relBuilder.call(
              SqlStdOperatorTable.CASE,
              context.relBuilder.call(SqlStdOperatorTable.LESS_THAN, startValue, minValue),
              startValue,
              minValue);
    }

    if (node.getEnd() != null) {
      RexNode endValue = rexVisitor.analyze(node.getEnd(), context);
      effectiveMax =
          context.relBuilder.call(
              SqlStdOperatorTable.CASE,
              context.relBuilder.call(SqlStdOperatorTable.GREATER_THAN, endValue, maxValue),
              endValue,
              maxValue);
    }

    RexNode effectiveRange =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, effectiveMax, effectiveMin);

    RexNode log10Range = context.relBuilder.call(SqlStdOperatorTable.LOG10, effectiveRange);
    RexNode floorLog = context.relBuilder.call(SqlStdOperatorTable.FLOOR, log10Range);

    RexNode isExactPowerOf10 =
        context.relBuilder.call(SqlStdOperatorTable.EQUALS, log10Range, floorLog);

    RexNode adjustedMagnitude =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            isExactPowerOf10,
            context.relBuilder.call(
                SqlStdOperatorTable.MINUS, floorLog, context.relBuilder.literal(1.0)),
            floorLog);

    RexNode selectedWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.POWER, context.relBuilder.literal(10.0), adjustedMagnitude);

    RexNode firstBinStart =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY,
            context.relBuilder.call(
                SqlStdOperatorTable.FLOOR,
                context.relBuilder.call(SqlStdOperatorTable.DIVIDE, effectiveMin, selectedWidth)),
            selectedWidth);

    RexNode binValue = calculateBinValue(fieldExpr, selectedWidth, firstBinStart, context);

    RexNode binEnd = context.relBuilder.call(SqlStdOperatorTable.PLUS, binValue, selectedWidth);
    return createRangeString(binValue, binEnd, selectedWidth, context);
  }

  /**
   * Creates default binning when no parameters are specified. Detects field type and uses
   * appropriate binning.
   */
  public static RexNode createDefaultRangeStrings(
      Bin node, RexNode fieldExpr, CalcitePlanContext context) {
    RelDataType fieldType = fieldExpr.getType();
    String fieldName = extractFieldName(node);

    if (shouldApplyTimeBinning(fieldName, fieldType)) {
      validateTimestampFieldExists(fieldName, context);

      return BinSpanFunction.createBinTimeSpanExpression(fieldExpr, 1, "h", 0, context);
    }

    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();

    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    RexNode log10Range = context.relBuilder.call(SqlStdOperatorTable.LOG10, dataRange);
    RexNode magnitude = context.relBuilder.call(SqlStdOperatorTable.FLOOR, log10Range);

    RexNode tenLiteral = context.relBuilder.literal(10.0);
    RexNode defaultWidth =
        context.relBuilder.call(SqlStdOperatorTable.POWER, tenLiteral, magnitude);

    RexNode widthInt = context.relBuilder.call(SqlStdOperatorTable.FLOOR, defaultWidth);

    RexNode binStartValue = calculateBinValue(fieldExpr, widthInt, context);
    RexNode binEndValue =
        context.relBuilder.call(SqlStdOperatorTable.PLUS, binStartValue, widthInt);

    return createRangeString(binStartValue, binEndValue, context);
  }

  /**
   * Checks if a field should receive time-based binning treatment. SPL behavior: Time-based binning
   * ONLY applies to the special @timestamp field.
   */
  public static boolean shouldApplyTimeBinning(String fieldName, RelDataType fieldType) {
    // SPL compatibility: Only @timestamp field gets time-based binning
    return TIMESTAMP_FIELD.equals(fieldName) && isTimeBasedField(fieldType);
  }

  /** Validates that @timestamp field exists in the dataset when time-based binning is requested. */
  public static void validateTimestampFieldExists(String fieldName, CalcitePlanContext context) {
    if (TIMESTAMP_FIELD.equals(fieldName)) {
      List<String> availableFields = context.relBuilder.peek().getRowType().getFieldNames();
      if (!availableFields.contains(TIMESTAMP_FIELD)) {
        throw new IllegalArgumentException(
            "Time-based binning requires @timestamp field in dataset. "
                + "Please ensure your data contains @timestamp field for time operations. "
                + "Available fields: "
                + availableFields);
      }
    }
  }

  /** Validates time-based operations on non-@timestamp fields. */
  public static void validateTimeBasedOperations(Bin node, String fieldName) {
    // Skip validation for @timestamp field - it's allowed to use time operations
    if (TIMESTAMP_FIELD.equals(fieldName)) {
      return;
    }

    if (node.getSpan() != null) {
      String spanStr = getOriginalLiteralValue(node.getSpan());

      if (spanStr != null && spanStr.matches(".*[hmsd]$")) {
        throw new IllegalArgumentException(
            String.format(
                "Time-based binning requires '@timestamp' field. "
                    + "Field '%s' does not support time operations. "
                    + "Use '@timestamp' for time binning or remove time parameters.",
                fieldName));
      }
    }

    // Check for aligntime parameter - only valid for @timestamp
    if (node.getAligntime() != null) {
      throw new IllegalArgumentException(
          String.format(
              "Time-based binning requires '@timestamp' field. "
                  + "Field '%s' does not support time operations. "
                  + "Use '@timestamp' for time binning or remove time parameters.",
              fieldName));
    }
  }

  /** Checks if the field type is time-based. */
  public static boolean isTimeBasedField(RelDataType fieldType) {
    // First check standard SQL time types
    if (fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP
        || fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || fieldType.getSqlTypeName() == SqlTypeName.BIGINT // epoch timestamps
        || fieldType.getSqlTypeName() == SqlTypeName.DATE) {
      return true;
    }

    // Check for OpenSearch UDT types (EXPR_TIMESTAMP mapped to VARCHAR)
    if (fieldType instanceof AbstractExprRelDataType<?> exprType) {
      ExprType udtType = exprType.getExprType();
      return udtType == ExprCoreType.TIMESTAMP
          || udtType == ExprCoreType.DATE
          || udtType == ExprCoreType.TIME;
    }

    // Check if type string contains EXPR_TIMESTAMP (for cases where instanceof check fails)
    return fieldType.toString().contains("EXPR_TIMESTAMP");
  }

  // === HELPER METHODS ===

  /** Creates binning calculation with a specific width. */
  private static RexNode calculateBinValue(
      RexNode fieldExpr, RexNode selectedWidth, CalcitePlanContext context) {
    return calculateBinValue(fieldExpr, selectedWidth, null, context);
  }

  /** Creates binning calculation with a specific width and optional first bin start. */
  private static RexNode calculateBinValue(
      RexNode fieldExpr, RexNode selectedWidth, RexNode firstBinStart, CalcitePlanContext context) {
    if (firstBinStart == null) {
      RexNode divided =
          context.relBuilder.call(SqlStdOperatorTable.DIVIDE, fieldExpr, selectedWidth);
      RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      return context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, selectedWidth);
    } else {
      RexNode adjustedField =
          context.relBuilder.call(SqlStdOperatorTable.MINUS, fieldExpr, firstBinStart);
      RexNode divided =
          context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedField, selectedWidth);
      RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      RexNode binIndex =
          context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, selectedWidth);
      return context.relBuilder.call(SqlStdOperatorTable.PLUS, binIndex, firstBinStart);
    }
  }

  /** Creates a formatted range string from start and end values. */
  private static RexNode createRangeString(
      RexNode binValue, RexNode binEnd, CalcitePlanContext context) {
    return createRangeString(binValue, binEnd, null, context);
  }

  /** Creates a formatted range string from start and end values with optional width formatting. */
  private static RexNode createRangeString(
      RexNode binValue, RexNode binEnd, RexNode width, CalcitePlanContext context) {
    RexNode dash = context.relBuilder.literal(DASH_SEPARATOR);

    RexNode binValueFormatted =
        width != null
            ? createFormattedValue(binValue, width, context)
            : context.relBuilder.cast(
                context.relBuilder.call(SqlStdOperatorTable.FLOOR, binValue), SqlTypeName.VARCHAR);
    RexNode binEndFormatted =
        width != null
            ? createFormattedValue(binEnd, width, context)
            : context.relBuilder.cast(
                context.relBuilder.call(SqlStdOperatorTable.FLOOR, binEnd), SqlTypeName.VARCHAR);

    RexNode firstConcat =
        context.relBuilder.call(SqlStdOperatorTable.CONCAT, binValueFormatted, dash);
    return context.relBuilder.call(SqlStdOperatorTable.CONCAT, firstConcat, binEndFormatted);
  }

  /** Creates a formatted value expression that shows integers without decimals when appropriate. */
  private static RexNode createFormattedValue(
      RexNode value, RexNode width, CalcitePlanContext context) {
    RexNode isIntegerWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, width, context.relBuilder.literal(1.0));

    RexNode integerValue =
        context.relBuilder.cast(
            context.relBuilder.cast(value, SqlTypeName.INTEGER), SqlTypeName.VARCHAR);

    RexNode decimalValue = context.relBuilder.cast(value, SqlTypeName.VARCHAR);

    return context.relBuilder.call(
        SqlStdOperatorTable.CASE, isIntegerWidth, integerValue, decimalValue);
  }

  /** Creates a binning expression that implements SPL's nice number algorithm. */
  private static RexNode createFallbackBinningExpression(
      RexNode fieldExpr, int requestedBins, CalcitePlanContext context) {

    RexNode selectedWidth = createDynamicWidthSelection(fieldExpr, requestedBins, context);

    RexNode divided = context.relBuilder.call(SqlStdOperatorTable.DIVIDE, fieldExpr, selectedWidth);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binValue =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, selectedWidth);

    RexNode binEnd = context.relBuilder.call(SqlStdOperatorTable.PLUS, binValue, selectedWidth);
    return createRangeString(binValue, binEnd, selectedWidth, context);
  }

  /** Creates dynamic width selection that implements SPL's exact nice number algorithm. */
  private static RexNode createDynamicWidthSelection(
      RexNode fieldExpr, int requestedBins, CalcitePlanContext context) {

    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();

    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    List<RexNode> caseOperands = new ArrayList<>();

    for (double width : SPL_NICE_WIDTHS) {
      RexNode widthLiteral = context.relBuilder.literal(width);

      RexNode theoreticalBins =
          context.relBuilder.call(
              SqlStdOperatorTable.CEIL,
              context.relBuilder.call(SqlStdOperatorTable.DIVIDE, dataRange, widthLiteral));

      RexNode maxModWidth =
          context.relBuilder.call(SqlStdOperatorTable.MOD, maxValue, widthLiteral);
      RexNode needsExtraBin =
          context.relBuilder.call(
              SqlStdOperatorTable.EQUALS, maxModWidth, context.relBuilder.literal(0));
      RexNode extraBin =
          context.relBuilder.call(
              SqlStdOperatorTable.CASE,
              needsExtraBin,
              context.relBuilder.literal(1),
              context.relBuilder.literal(0));
      RexNode actualBins =
          context.relBuilder.call(SqlStdOperatorTable.PLUS, theoreticalBins, extraBin);

      RexNode constraint =
          context.relBuilder.call(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              actualBins,
              context.relBuilder.literal(requestedBins));

      caseOperands.add(constraint);
      caseOperands.add(widthLiteral);
    }

    RexNode exactWidth =
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, dataRange, context.relBuilder.literal(requestedBins));
    caseOperands.add(exactWidth);

    return context.relBuilder.call(SqlStdOperatorTable.CASE, caseOperands);
  }

  /** Parses time span string (like "1h", "30seconds") and creates bin-specific span expression. */
  private static RexNode createBinSpanExpressionFromString(
      String spanStr, RexNode fieldExpr, RexNode alignTimeValue, CalcitePlanContext context) {

    // Parse alignment offset if provided
    long alignmentOffsetMillis = 0;
    if (alignTimeValue instanceof RexLiteral literal) {
      Object value = literal.getValue();

      if (value instanceof String) {
        alignmentOffsetMillis = parseAlignTimeOffset((String) value);
      } else if (value instanceof Number) {
        alignmentOffsetMillis = ((Number) value).longValue() * 1000L;
      }
    }

    // Parse time span and create bin-specific expression
    try {
      // Clean up the span string
      spanStr = spanStr.replace("'", "").replace("\"", "").trim();

      // Use the comprehensive time unit extraction
      String timeUnit = extractTimeUnit(spanStr);
      if (timeUnit != null) {
        String valueStr = spanStr.substring(0, spanStr.length() - timeUnit.length());
        int value = Integer.parseInt(valueStr);

        // Normalize the unit to BinSpanFunction's expected format
        String normalizedUnit = normalizeTimeUnit(timeUnit);
        return BinSpanFunction.createBinTimeSpanExpression(
            fieldExpr, value, normalizedUnit, alignmentOffsetMillis, context);
      } else {
        // Try parsing as pure number (assume hours)
        int value = Integer.parseInt(spanStr);
        return BinSpanFunction.createBinTimeSpanExpression(
            fieldExpr, value, "h", alignmentOffsetMillis, context);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid time span format: '" + spanStr + "'", e);
    }
  }

  /** Normalizes time units to the format expected by BinSpanFunction. */
  private static String normalizeTimeUnit(String unit) {
    // Convert all unit variations to the canonical form that BinSpanFunction expects
    switch (unit.toLowerCase()) {
        // Seconds
      case "s":
      case "sec":
      case "secs":
      case "second":
      case "seconds":
        return "s";
        // Minutes
      case "m":
      case "min":
      case "mins":
      case "minute":
      case "minutes":
        return "m";
        // Hours
      case "h":
      case "hr":
      case "hrs":
      case "hour":
      case "hours":
        return "h";
        // Days
      case "d":
      case "day":
      case "days":
        return "d";
        // Months (case-sensitive M)
      case "M":
      case "mon":
      case "month":
      case "months":
        return "M";
        // Subseconds
      case "us":
        return "us";
      case "ms":
        return "ms";
      case "cs":
        return "cs";
      case "ds":
        return "ds";
      default:
        return unit; // Return as-is if not recognized
    }
  }

  /** Creates a CASE expression that converts numeric values to range strings. */
  private static RexNode createRangeCaseExpression(
      RexNode fieldExpr,
      int span,
      RexNode alignTimeValue,
      CalcitePlanContext context,
      org.opensearch.sql.ast.expression.UnresolvedExpression startExpr,
      org.opensearch.sql.ast.expression.UnresolvedExpression endExpr,
      org.opensearch.sql.calcite.CalciteRexNodeVisitor rexVisitor) {

    int rangeStart = 0;
    if (alignTimeValue instanceof RexLiteral literal
        && literal.getValue() instanceof Number number) {
      rangeStart = number.intValue();
    }

    // Override with start parameter if specified
    if (startExpr != null) {
      RexNode startValue = rexVisitor.analyze(startExpr, context);
      if (startValue.isA(LITERAL)) {
        rangeStart = ((Number) ((RexLiteral) startValue).getValue()).intValue();
      }
    }

    RexNode spanLiteral = context.relBuilder.literal(span);
    RexNode rangeStartLiteral = context.relBuilder.literal(rangeStart);

    // Calculate: (field - rangeStart) / span
    RexNode adjustedField =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, fieldExpr, rangeStartLiteral);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedField, spanLiteral);

    // Floor the division to get bin number
    RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);

    // Calculate bin_start = binNumber * span + rangeStart
    RexNode binStartValue =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS,
            context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, binNumber, spanLiteral),
            rangeStartLiteral);

    // Calculate bin_end = bin_start + span
    RexNode binEndValue =
        context.relBuilder.call(SqlStdOperatorTable.PLUS, binStartValue, spanLiteral);

    // Create range string
    RexNode rangeString = createRangeString(binStartValue, binEndValue, context);

    // If end parameter is specified, we need to handle values outside the range
    if (endExpr != null) {
      RexNode endValue = rexVisitor.analyze(endExpr, context);
      if (endValue instanceof RexLiteral literal && literal.getValue() instanceof Number number) {
        int rangeEnd = number.intValue();

        // Create condition: field >= rangeStart AND field < rangeEnd
        RexNode inRangeCondition =
            context.relBuilder.call(
                SqlStdOperatorTable.AND,
                context.relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    fieldExpr,
                    context.relBuilder.literal(rangeStart)),
                context.relBuilder.call(
                    SqlStdOperatorTable.LESS_THAN,
                    fieldExpr,
                    context.relBuilder.literal(rangeEnd)));

        // Return CASE WHEN in_range THEN range_string ELSE 'Other' END
        return context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            inRangeCondition,
            rangeString,
            context.relBuilder.literal(OTHER_CATEGORY));
      }
    }

    // No end limit - return the dynamic range string for all values
    return rangeString;
  }

  /**
   * Creates a span-based expression that handles different span types: - Numeric spans (e.g.,
   * "1000") - Log-based spans (e.g., "log10", "2log10")
   */
  private static RexNode createSpanBasedExpression(
      String spanStr, RexNode fieldExpr, CalcitePlanContext context) {
    try {
      SpanInfo spanInfo = parseSpanString(spanStr);

      switch (spanInfo.type) {
        case NUMERIC -> {
          // Traditional numeric span - use the range case expression
          return createRangeCaseExpression(
              fieldExpr, (int) spanInfo.value, null, context, null, null, null);
        }
        case LOG -> {
          // Logarithmic binning
          return createLogSpanExpression(fieldExpr, spanInfo, context);
        }
        default -> throw new IllegalArgumentException("Unsupported span type: " + spanInfo.type);
      }
    } catch (Exception e) {
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new IllegalArgumentException(
          "Failed to parse span: " + spanStr + ". " + errorMessage, e);
    }
  }

  /** Create logarithmic span expression using SPL-compatible data-driven approach */
  private static RexNode createLogSpanExpression(
      RexNode fieldExpr, SpanInfo spanInfo, CalcitePlanContext context) {
    double base = spanInfo.base;
    double coefficient = spanInfo.coefficient;

    // Check if value is positive (log only defined for positive numbers)
    RexNode positiveCheck =
        context.relBuilder.call(
            SqlStdOperatorTable.GREATER_THAN, fieldExpr, context.relBuilder.literal(0.0));

    // Apply coefficient: adjusted_value = field_value / coefficient
    RexNode adjustedField = fieldExpr;
    if (coefficient != 1.0) {
      adjustedField =
          context.relBuilder.call(
              SqlStdOperatorTable.DIVIDE, fieldExpr, context.relBuilder.literal(coefficient));
    }

    // Calculate log_base(adjusted_field) = ln(adjusted_field) / ln(base)
    RexNode lnField = context.relBuilder.call(SqlStdOperatorTable.LN, adjustedField);
    RexNode lnBase = context.relBuilder.literal(Math.log(base));
    RexNode logValue = context.relBuilder.call(SqlStdOperatorTable.DIVIDE, lnField, lnBase);

    // Get the bin number by flooring the log value
    RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, logValue);

    RexNode baseNode = context.relBuilder.literal(base);
    RexNode coefficientNode = context.relBuilder.literal(coefficient);

    RexNode basePowerBin = context.relBuilder.call(SqlStdOperatorTable.POWER, baseNode, binNumber);
    RexNode lowerBound =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, coefficientNode, basePowerBin);

    RexNode binPlusOne =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS, binNumber, context.relBuilder.literal(1.0));
    RexNode basePowerBinPlusOne =
        context.relBuilder.call(SqlStdOperatorTable.POWER, baseNode, binPlusOne);
    RexNode upperBound =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, coefficientNode, basePowerBinPlusOne);

    // Create range string
    RexNode rangeStr = createRangeString(lowerBound, upperBound, context);

    // Return range for positive values, "Invalid" for non-positive
    return context.relBuilder.call(
        SqlStdOperatorTable.CASE,
        positiveCheck,
        rangeStr,
        context.relBuilder.literal(INVALID_CATEGORY));
  }

  /** Extracts the original string value from a literal expression before RexVisitor processing. */
  private static String getOriginalLiteralValue(
      org.opensearch.sql.ast.expression.UnresolvedExpression expr) {
    if (expr instanceof org.opensearch.sql.ast.expression.Literal) {
      org.opensearch.sql.ast.expression.Literal literal =
          (org.opensearch.sql.ast.expression.Literal) expr;
      return literal.getValue().toString();
    }
    return null;
  }

  /**
   * Parses aligntime expressions like "@d", "@d+3h", "@d-1h" into millisecond offsets from start of
   * day.
   */
  private static long parseAlignTimeOffset(String alignTimeStr) {
    if (alignTimeStr == null) {
      return 0;
    }

    alignTimeStr = alignTimeStr.replace("'", "").replace("\"", "").trim();

    if ("@d".equals(alignTimeStr)) {
      return 0;
    }

    if (alignTimeStr.startsWith("@d+")) {
      String offsetStr = alignTimeStr.substring(3); // Remove "@d+"
      return parseTimeOffset(offsetStr);
    }

    if (alignTimeStr.startsWith("@d-")) {
      String offsetStr = alignTimeStr.substring(3); // Remove "@d-"
      return -parseTimeOffset(offsetStr);
    }

    return 0; // Default to start of day
  }

  /** Helper method to parse time value from string with unit. */
  private static int parseTimeValue(String valueStr, String unit) {
    return Integer.parseInt(valueStr.substring(0, valueStr.length() - unit.length()));
  }

  /** Parses time offset expressions like "3h", "30m", "45s" into milliseconds. */
  private static long parseTimeOffset(String offsetStr) {
    offsetStr = offsetStr.trim().toLowerCase();

    if (offsetStr.endsWith("h")) {
      int hours = parseTimeValue(offsetStr, "h");
      return hours * MILLISECONDS_PER_HOUR;
    }

    if (offsetStr.endsWith("m")) {
      int minutes = parseTimeValue(offsetStr, "m");
      return minutes * MILLISECONDS_PER_MINUTE;
    }

    if (offsetStr.endsWith("s")) {
      int seconds = parseTimeValue(offsetStr, "s");
      return seconds * MILLISECONDS_PER_SECOND;
    }

    // Default to hours if no unit specified
    int hours = Integer.parseInt(offsetStr);
    return hours * MILLISECONDS_PER_HOUR;
  }

  // === SPAN OPTIONS IMPLEMENTATION (for tests) ===

  /** Enum for different span types */
  public enum SpanType {
    LOG, // Logarithmic span (e.g., log10, 2log10)
    TIME, // Time-based span (e.g., 30seconds, 15minutes)
    NUMERIC // Numeric span (existing behavior)
  }

  /** Data class to hold parsed span information */
  public static class SpanInfo {
    public final SpanType type;
    public final double value;
    public final String unit;
    public final double coefficient; // For log spans
    public final double base; // For log spans

    public SpanInfo(SpanType type, double value, String unit) {
      this.type = type;
      this.value = value;
      this.unit = unit;
      this.coefficient = 1.0;
      this.base = 10.0;
    }

    public SpanInfo(SpanType type, double coefficient, double base) {
      this.type = type;
      this.value = 0;
      this.unit = null;
      this.coefficient = coefficient;
      this.base = base;
    }
  }

  /** Parse span string to determine type and extract parameters */
  public static SpanInfo parseSpanString(String spanStr) {
    String lowerSpanStr = spanStr.toLowerCase().trim();

    // Special handling for common log spans
    switch (lowerSpanStr) {
      case "log10" -> {
        return new SpanInfo(SpanType.LOG, 1.0, 10.0);
      }
      case "log2" -> {
        return new SpanInfo(SpanType.LOG, 1.0, 2.0);
      }
      case "loge", "ln" -> {
        return new SpanInfo(SpanType.LOG, 1.0, Math.E);
      }
    }

    Pattern logPattern = Pattern.compile("^(\\d*\\.?\\d*)?log(\\d+\\.?\\d*)$");
    Matcher logMatcher = logPattern.matcher(lowerSpanStr);

    if (logMatcher.matches()) {
      String coeffStr = logMatcher.group(1);
      String baseStr = logMatcher.group(2);

      double coefficient =
          (coeffStr == null || coeffStr.isEmpty()) ? 1.0 : Double.parseDouble(coeffStr);
      double base = Double.parseDouble(baseStr);

      // Validate log span parameters
      if (base <= 1.0) {
        throw new IllegalArgumentException("Log base must be > 1.0, got: " + base);
      }
      if (coefficient <= 0.0) {
        throw new IllegalArgumentException(
            "Log coefficient must be > 0.0, got coefficient=" + coefficient + ", base=" + base);
      }

      return new SpanInfo(SpanType.LOG, coefficient, base);
    }

    // Time-based span patterns
    String timeUnit = extractTimeUnit(spanStr);
    if (timeUnit != null) {
      String valueStr = spanStr.substring(0, spanStr.length() - timeUnit.length());
      double value = Double.parseDouble(valueStr);
      return new SpanInfo(SpanType.TIME, value, timeUnit);
    }

    // Numeric span (fallback)
    try {
      double value = Double.parseDouble(spanStr);
      return new SpanInfo(SpanType.NUMERIC, value, null);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid span format: " + spanStr);
    }
  }

  /** Extract time unit from span string following SPL timescale specification. */
  public static String extractTimeUnit(String spanStr) {
    // SPL Timescale units in order of precedence (longest first to avoid partial matches)
    String[] timeUnits = {
      // Order by length (longest first) to avoid partial matches

      // <sec> - seconds (full words first)
      "seconds",
      "second",
      "secs",
      "sec",

      // <min> - minutes (full words first)
      "minutes",
      "minute",
      "mins",
      "min",

      // <hr> - hours (full words first)
      "hours",
      "hour",
      "hrs",
      "hr",

      // <day> - days
      "days",
      "day",

      // <month> - months (case-sensitive M for months vs m for minutes)
      "months",
      "month",
      "mon",
      "M", // Case-sensitive: M = months, m = minutes

      // <subseconds> - microseconds, milliseconds, centiseconds, deciseconds
      "us", // microseconds
      "ms", // milliseconds
      "cs", // centiseconds
      "ds", // deciseconds

      // Single letter units (must be last to avoid conflicts)
      "s",
      "m",
      "h",
      "d"
    };

    // Case-sensitive matching for M (months) vs m (minutes)
    for (String unit : timeUnits) {
      if (unit.equals("M")) {
        // Case-sensitive check for months
        if (spanStr.endsWith("M")) {
          return unit;
        }
      } else {
        // Case-insensitive check for other units
        if (spanStr.toLowerCase().endsWith(unit.toLowerCase())) {
          return unit;
        }
      }
    }
    return null;
  }
}
