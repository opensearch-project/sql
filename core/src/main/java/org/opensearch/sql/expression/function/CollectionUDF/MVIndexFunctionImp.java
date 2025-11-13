/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDFUNCTION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_SLICE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.INTERNAL_ITEM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LESS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACT;

import java.math.BigDecimal;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * MVINDEX function implementation that returns a subset of a multivalue array.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>mvindex(array, start) - returns single element at index (0-based)
 *   <li>mvindex(array, start, end) - returns array slice from start to end (inclusive, 0-based)
 * </ul>
 *
 * <p>Supports negative indexing where -1 refers to the last element.
 *
 * <p>Implementation notes:
 *
 * <ul>
 *   <li>Single element access uses Calcite's ITEM operator (1-based indexing)
 *   <li>Range access uses Calcite's ARRAY_SLICE operator (0-based indexing with length parameter)
 *   <li>Index conversion handles the difference between PPL's 0-based indexing and Calcite's
 *       conventions
 * </ul>
 */
public class MVIndexFunctionImp implements PPLFuncImpTable.FunctionImp {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode... args) {
    RexNode array = args[0];
    RexNode startIdx = args[1];

    // Use resolve to get array length instead of direct makeCall
    RexNode arrayLen = PPLFuncImpTable.INSTANCE.resolve(builder, ARRAY_LENGTH, array);

    if (args.length == 2) {
      // Single element access using ITEM (1-based indexing)
      return resolveSingleElement(builder, array, startIdx, arrayLen);
    } else {
      // Range access using ARRAY_SLICE (0-based indexing)
      RexNode endIdx = args[2];
      return resolveRange(builder, array, startIdx, endIdx, arrayLen);
    }
  }

  /**
   * Resolves single element access: mvindex(array, index)
   *
   * <p>Uses Calcite's ITEM operator which uses 1-based indexing. Converts PPL's 0-based index to
   * 1-based by adding 1.
   */
  private RexNode resolveSingleElement(
      RexBuilder builder, RexNode array, RexNode startIdx, RexNode arrayLen) {
    // Convert 0-based PPL index to 1-based Calcite ITEM index
    RexNode zero = builder.makeExactLiteral(BigDecimal.ZERO);
    RexNode one = builder.makeExactLiteral(BigDecimal.ONE);

    RexNode isNegative = PPLFuncImpTable.INSTANCE.resolve(builder, LESS, startIdx, zero);
    RexNode sumArrayLenStart =
        PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, arrayLen, startIdx);
    RexNode negativeCase =
        PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, sumArrayLenStart, one);
    RexNode positiveCase = PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, startIdx, one);

    RexNode normalizedStart =
        PPLFuncImpTable.INSTANCE.resolve(builder, IF, isNegative, negativeCase, positiveCase);

    return PPLFuncImpTable.INSTANCE.resolve(builder, INTERNAL_ITEM, array, normalizedStart);
  }

  /**
   * Resolves range access: mvindex(array, start, end)
   *
   * <p>Uses Calcite's ARRAY_SLICE operator which uses 0-based indexing and a length parameter.
   * PPL's end index is inclusive, so length = (end - start) + 1.
   */
  private RexNode resolveRange(
      RexBuilder builder, RexNode array, RexNode startIdx, RexNode endIdx, RexNode arrayLen) {
    // Normalize negative indices for ARRAY_SLICE (0-based)
    RexNode zero = builder.makeExactLiteral(BigDecimal.ZERO);
    RexNode one = builder.makeExactLiteral(BigDecimal.ONE);

    RexNode isStartNegative = PPLFuncImpTable.INSTANCE.resolve(builder, LESS, startIdx, zero);
    RexNode startNegativeCase =
        PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, arrayLen, startIdx);
    RexNode normalizedStart =
        PPLFuncImpTable.INSTANCE.resolve(builder, IF, isStartNegative, startNegativeCase, startIdx);

    RexNode isEndNegative = PPLFuncImpTable.INSTANCE.resolve(builder, LESS, endIdx, zero);
    RexNode endNegativeCase =
        PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, arrayLen, endIdx);
    RexNode normalizedEnd =
        PPLFuncImpTable.INSTANCE.resolve(builder, IF, isEndNegative, endNegativeCase, endIdx);

    // Calculate length: (normalizedEnd - normalizedStart) + 1
    RexNode diff =
        PPLFuncImpTable.INSTANCE.resolve(builder, SUBTRACT, normalizedEnd, normalizedStart);
    RexNode length = PPLFuncImpTable.INSTANCE.resolve(builder, ADDFUNCTION, diff, one);

    // Call ARRAY_SLICE(array, normalizedStart, length)
    return PPLFuncImpTable.INSTANCE.resolve(builder, ARRAY_SLICE, array, normalizedStart, length);
  }
}
