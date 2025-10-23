/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

/**
 * Analyzer to detect CASE expressions that can be converted to OpenSearch range aggregations.
 *
 * <p>Strict validation rules:
 *
 * <ul>
 *   <li>All conditions must compare the same field with literals
 *   <li>Only closed-open, at-least, and less-than ranges are allowed
 *   <li>Return values must be string literals
 * </ul>
 */
public class CaseRangeAnalyzer {
  /** The default key to use if there isn't a key specified for the else case */
  public static final String DEFAULT_ELSE_KEY = "null";

  private final RelDataType rowType;
  private final RangeSet<Double> takenRange;
  private final RangeAggregationBuilder builder;

  public CaseRangeAnalyzer(String name, RelDataType rowType) {
    this.rowType = rowType;
    this.takenRange = TreeRangeSet.create();
    this.builder = AggregationBuilders.range(name).keyed(true);
  }

  /**
   * Creates a new CaseRangeAnalyzer instance.
   *
   * @param rowType the row type information for field resolution
   * @return a new CaseRangeAnalyzer instance
   */
  public static CaseRangeAnalyzer create(String name, RelDataType rowType) {
    return new CaseRangeAnalyzer(name, rowType);
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

    // Process WHEN-THEN pairs
    for (int i = 0; i < operands.size() - 1; i += 2) {
      RexNode condition = operands.get(i);
      RexNode expr = operands.get(i + 1);
      try {
        String key = parseLiteralAsString(expr);
        analyzeCondition(condition, key);
      } catch (UnsupportedOperationException e) {
        return Optional.empty();
      }
    }

    // Check ELSE clause
    RexNode elseExpr = operands.getLast();
    String elseKey;
    if (RexLiteral.isNullLiteral(elseExpr)) {
      // range key doesn't support values of type: VALUE_NULL
      elseKey = DEFAULT_ELSE_KEY;
    } else {
      try {
        elseKey = parseLiteralAsString(elseExpr);
      } catch (UnsupportedOperationException e) {
        return Optional.empty();
      }
    }
    addRangeSet(elseKey, takenRange.complement());
    return Optional.of(builder);
  }

  /** Analyzes a single condition in the CASE WHEN clause. */
  private void analyzeCondition(RexNode condition, String key) {
    if (!(condition instanceof RexCall)) {
      throwUnsupported("condition must be a RexCall");
    }

    RexCall call = (RexCall) condition;
    SqlKind kind = call.getKind();

    // Handle simple comparisons
    if (kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL
        || kind == SqlKind.GREATER_THAN) {
      analyzeSimpleComparison(call, key);
    } else if (kind == SqlKind.SEARCH) {
      analyzeSearchCondition(call, key);
    }
    // AND / OR will only appear when users try to create a complex condition on multiple fields
    // E.g. (a > 3 and b < 5). Otherwise, the complex conditions will be converted to a SEARCH call.
    else if (kind == SqlKind.AND || kind == SqlKind.OR) {
      throwUnsupported("Range queries must be performed on the same field");
    } else {
      throwUnsupported("Can not analyze condition as a range query: " + call);
    }
  }

  private void analyzeSimpleComparison(RexCall call, String key) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 || !(call.getOperator() instanceof SqlBinaryOperator)) {
      throwUnsupported();
    }
    RexNode left = operands.get(0);
    RexNode right = operands.get(1);
    SqlOperator operator = call.getOperator();
    RexInputRef inputRef = null;
    RexLiteral literal = null;

    // Swap inputRef to the left if necessary
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
    if (value == null) {
      throwUnsupported("Cannot parse value for comparison");
    }
    switch (operator.getKind()) {
      case GREATER_THAN_OR_EQUAL -> {
        addFrom(key, value);
      }
      case LESS_THAN -> {
        addTo(key, value);
      }
      default -> throw new UnsupportedOperationException(
          "ranges must be equivalents of field >= constant or field < constant");
    }
    ;
  }

  private void analyzeSearchCondition(RexCall searchCall, String key) {
    RexNode field = searchCall.getOperands().getFirst();
    if (!(field instanceof RexInputRef)) {
      throwUnsupported("Range query must be performed on a field");
    }
    String fieldName = getFieldName((RexInputRef) field);
    if (builder.field() == null) {
      builder.field(fieldName);
    } else if (!Objects.equals(builder.field(), fieldName)) {
      throwUnsupported("Range query must be performed on the same field");
    }
    RexLiteral literal = (RexLiteral) searchCall.getOperands().getLast();
    Sarg<?> sarg = Objects.requireNonNull(literal.getValueAs(Sarg.class));
    for (Range<?> r : sarg.rangeSet.asRanges()) {
      @SuppressWarnings("unchecked")
      Range<BigDecimal> range = (Range<BigDecimal>) r;
      validateRange(range);
      if (!range.hasLowerBound() && range.hasUpperBound()) {
        // It will be Double.MAX_VALUE if be big decimal is greater than Double.MAX_VALUE
        double upper = range.upperEndpoint().doubleValue();
        addTo(key, upper);
      } else if (range.hasLowerBound() && !range.hasUpperBound()) {
        double lower = range.lowerEndpoint().doubleValue();
        addFrom(key, lower);
      } else if (range.hasLowerBound()) {
        double lower = range.lowerEndpoint().doubleValue();
        double upper = range.upperEndpoint().doubleValue();
        addBetween(key, lower, upper);
      } else {
        addBetween(key, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
      }
    }
  }

  private void addFrom(String key, Double value) {
    var from = Range.atLeast(value);
    updateRange(key, from);
  }

  private void addTo(String key, Double value) {
    var to = Range.lessThan(value);
    updateRange(key, to);
  }

  private void addBetween(String key, Double from, Double to) {
    var range = Range.closedOpen(from, to);
    updateRange(key, range);
  }

  private void updateRange(String key, Range<Double> range) {
    // The range to add: remaining space ∩ new range
    RangeSet<Double> toAdd = takenRange.complement().subRangeSet(range);
    addRangeSet(key, toAdd);
    takenRange.add(range);
  }

  // Add range set without updating taken range
  private void addRangeSet(String key, RangeSet<Double> rangeSet) {
    rangeSet.asRanges().forEach(range -> addRange(key, range));
  }

  // Add range without updating taken range
  private void addRange(String key, Range<Double> range) {
    validateRange(range);
    if (range.hasLowerBound() && range.hasUpperBound()) {
      builder.addRange(key, range.lowerEndpoint(), range.upperEndpoint());
    } else if (range.hasLowerBound()) {
      builder.addUnboundedFrom(key, range.lowerEndpoint());
    } else if (range.hasUpperBound()) {
      builder.addUnboundedTo(key, range.upperEndpoint());
    } else {
      builder.addRange(key, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }
  }

  private String getFieldName(RexInputRef field) {
    return rowType.getFieldNames().get(field.getIndex());
  }

  private static void validateRange(Range<?> range) {
    if ((range.hasLowerBound() && range.lowerBoundType() != BoundType.CLOSED)
        || (range.hasUpperBound() && range.upperBoundType() != BoundType.OPEN)) {
      throwUnsupported("Range query only supports closed-open ranges");
    }
  }

  private static String parseLiteralAsString(RexNode node) {
    if (!(node instanceof RexLiteral)) {
      throwUnsupported("Result expressions of range queries must be literals");
    }
    RexLiteral literal = (RexLiteral) node;
    try {
      return literal.getValueAs(String.class);
    } catch (AssertionError ignore) {
    }
    throw new UnsupportedOperationException(
        "Cannot parse result expression of type " + literal.getType());
  }

  private static void throwUnsupported() {
    throw new UnsupportedOperationException("Cannot create range aggregator from case");
  }

  private static void throwUnsupported(String message) {
    throw new UnsupportedOperationException("Cannot create range aggregator: " + message);
  }
}
