/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Sarg;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;

/**
 * Analyzer to detect CASE expressions that can be converted to OpenSearch range aggregations.
 *
 * <p>Strict validation rules:
 * <ul>
 *   <li>All conditions must compare the same field with literals</li>
 *   <li>Only simple comparison operators (>, >=, <, <=) are allowed</li>
 *   <li>Ranges must be non-overlapping and contiguous</li>
 *   <li>Return values must be string literals</li>
 * </ul>
 */
public class CaseRangeAnalyzer {
  private final RelDataType rowType;
  private final RangeSet<BigDecimal> rangeSet;

  public CaseRangeAnalyzer(RelDataType rowType) {
    this.rowType = rowType;
    this.rangeSet = TreeRangeSet.create();
  }

  /**
   * Creates a new CaseRangeAnalyzer instance.
   *
   * @param rowType the row type information for field resolution
   * @return a new CaseRangeAnalyzer instance
   */
  public static CaseRangeAnalyzer create(RelDataType rowType) {
    return new CaseRangeAnalyzer(rowType);
  }

  /**
   * Analyzes a CASE expression to determine if it can be converted to a range aggregation.
   *
   * @param caseCall The CASE RexCall to analyze
   * @return Optional RangeAggregationBuilder if conversion is possible, empty otherwise
   */
  public Optional<RangeAggregationBuilder> analyze(RexCall caseCall) {
    if (!caseCall.getKind().equals(SqlKind.CASE)) {
      return Optional.empty();
    }

    List<RexNode> operands = caseCall.getOperands();
    RangeAggregationBuilder aggregationBuilder = AggregationBuilders.range("case_range");

    // Process WHEN-THEN pairs
    for (int i = 0; i < operands.size() - 1; i += 2) {
      RexNode condition = operands.get(i);
      RexNode result = operands.get(i + 1);
      // Result must be a literal
      if (!(result instanceof RexLiteral)) {
        return Optional.empty();
      }
      String key = ((RexLiteral) result).getValueAs(String.class);
      analyzeCondition(aggregationBuilder, condition, key);
    }

    // Check ELSE clause
    // TODO: Currently, we ignore else clause
    //  Process the case without else clause and check range completeness later
    return Optional.of(aggregationBuilder);
  }

  /** Analyzes a single condition in the CASE WHEN clause. */
  private void analyzeCondition(RangeAggregationBuilder builder, RexNode condition, String key) {
    if (!(condition instanceof RexCall)) {
      throwUnsupported("condition must be a RexCall");
    }

    RexCall call = (RexCall) condition;
    SqlKind kind = call.getKind();

    // Handle simple comparisons
    if (kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL || kind == SqlKind.GREATER_THAN) {
      builder.addRange(analyzeSimpleComparison(builder, call, key));
    }
    // Handle AND conditions (for range conditions like x >= 10 AND x < 100)
    else if (kind == SqlKind.AND || kind == SqlKind.OR) {
      analyzeCompositeCondition(builder, call, key);
    } else if (kind == SqlKind.SEARCH) {
      analyzeSearchCondition(builder, call, key);
    }
  }

  private RangeAggregator.Range analyzeSimpleComparison(RangeAggregationBuilder builder, RexCall call, String key) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 || !(call.getOperator() instanceof SqlBinaryOperator)) {
      throwUnsupported();
    }
    RexNode left = operands.get(0);
    RexNode right = operands.get(1);
    SqlOperator operator =  call.getOperator();
    RexInputRef inputRef = null;
    RexLiteral literal = null;

    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      inputRef = (RexInputRef) left;
      literal = (RexLiteral) right;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      inputRef = (RexInputRef) right;
      literal = (RexLiteral) left;
      operator = operator.reverse();
    } else {
      throwUnsupported();
    }

    if (operator == null) {
      throwUnsupported();
    }

    String fieldName = rowType.getFieldNames().get(inputRef.getIndex());
    if (builder.field() == null) {
      builder.field(fieldName);
    } else if (!Objects.equals(builder.field(), fieldName)) {
      throwUnsupported("comparison must be performed on the same field");
    }

    Double value = literal.getValueAs(Double.class);
    return switch (operator.getKind()) {
      case GREATER_THAN_OR_EQUAL -> new RangeAggregator.Range(key, value, null);
      case LESS_THAN -> new RangeAggregator.Range(key, null, value);
      default -> throw new UnsupportedOperationException("ranges must equivalents of field >= constant or field < constant");
    };
  }

  private void analyzeCompositeCondition(RangeAggregationBuilder builder, RexCall compositeCall, String key) {
    RexNode left = compositeCall.getOperands().get(0);
    RexNode right = compositeCall.getOperands().get(1);

    if (!(left instanceof RexCall && right instanceof RexCall && ((RexCall) left).getOperator() instanceof SqlBinaryOperator && ((RexCall) right).getOperator() instanceof SqlBinaryOperator)) {
      throwUnsupported("cannot analyze deep nested comparison");
    }

    // For AND conditions, we need to analyze them separately and combine
    // Create temporary ranges to analyze the conditions
    RangeAggregator.Range leftRange = analyzeSimpleComparison(builder, (RexCall) left, key);
    RangeAggregator.Range rightRange = analyzeSimpleComparison(builder, (RexCall) right, key);

    // Combine into single range
    if (compositeCall.getKind() == SqlKind.AND) {
      and(builder, leftRange, rightRange, key);
    } else if (compositeCall.getKind() == SqlKind.OR) {
      or(builder, leftRange, rightRange, key);
    }
  }

  private void analyzeSearchCondition(RangeAggregationBuilder builder, RexCall searchCall, String key) {
    RexNode field = searchCall.getOperands().getFirst();
    if (!(field instanceof RexInputRef) || !Objects.equals(getFieldName((RexInputRef) field), builder.field())) {
      throwUnsupported("Range query must be performed on the same field");
    }
    RexLiteral literal = (RexLiteral) searchCall.getOperands().getLast();
    Sarg<?> sarg = literal.getValueAs(Sarg.class);
    for(Object r: sarg.rangeSet.asRanges()){
      Range<BigDecimal> range = (Range<BigDecimal>) r;
      if ((range.hasLowerBound() && range.lowerBoundType() != BoundType.CLOSED) || (range.hasUpperBound() && range.upperBoundType() != BoundType.OPEN)){
        throwUnsupported("Range query only supports closed-open ranges");
      }
      if (!range.hasLowerBound() && range.hasUpperBound()) {
        builder.addUnboundedTo(key, range.upperEndpoint().doubleValue());
      } else if (range.hasLowerBound() && !range.hasUpperBound()) {
        builder.addUnboundedFrom(key, range.lowerEndpoint().doubleValue());
      } else if (range.hasLowerBound()) {
        builder.addRange(key, range.lowerEndpoint().doubleValue(), range.upperEndpoint().doubleValue());
      } else {
        builder.addRange(key, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
      }
    }
  }

  private static void and(
          RangeAggregationBuilder builder, RangeAggregator.Range left, RangeAggregator.Range right, String key) {
    double mergedFrom = Math.max(left.getFrom(), right.getFrom());
    double mergedTo = Math.min(left.getTo(), right.getTo());
    if (mergedFrom > Double.NEGATIVE_INFINITY && mergedTo < Double.POSITIVE_INFINITY) {
      // Closed range: both bounds are finite
      builder.addRange(key, mergedFrom, mergedTo);
    } else if (mergedFrom > Double.NEGATIVE_INFINITY) {
      // Unbounded from: only lower bound (e.g., x >= 10)
      builder.addUnboundedFrom(key, mergedFrom);
    } else if (mergedTo < Double.POSITIVE_INFINITY) {
      // Unbounded to: only upper bound (e.g., x < 50)
      builder.addUnboundedTo(key, mergedTo);
    } // If no overlapping, do nothing
  }

  private static void or(
          RangeAggregationBuilder builder, RangeAggregator.Range left, RangeAggregator.Range right, String key) {
    // sort left and right by swapping if necessary
    if(right.getFrom() < left.getFrom() || (left.getFrom() == right.getFrom() && right.getTo() < left.getTo())) {
      var tmp = right;
      right = left;
      left = tmp;
    }
    boolean overlap = left.getTo() > right.getFrom();
    if (overlap) {
      // Ranges overlap, meaning they cover all ranges - add both unbounded ranges
      double mergedFrom = Math.min(left.getFrom(), right.getFrom());
      double mergedTo = Math.max(left.getTo(), right.getTo());
      builder.addRange(key, mergedFrom, mergedTo);
    } else {
      builder.addRange(left);
      builder.addRange(right);
    }
  }

  private String getFieldName(RexInputRef field) {
    return rowType.getFieldNames().get(field.getIndex());
  }

  private static void throwUnsupported() {
    throw new UnsupportedOperationException("Cannot create range aggregator");
  }

  private static void throwUnsupported(String message) {
    throw new UnsupportedOperationException("Cannot create range aggregator: " + message);
  }
}