/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/** Function signature is composed by function name and arguments list. */
public final class CalciteFuncSignature {

  private final FunctionName functionName;
  private final PPLTypeChecker typeChecker;

  public CalciteFuncSignature(FunctionName functionName, PPLTypeChecker typeChecker) {
    this.functionName = functionName;
    this.typeChecker = typeChecker;
  }

  public FunctionName getFunctionName() {
    return functionName;
  }

  public PPLTypeChecker getTypeChecker() {
    return typeChecker;
  }

  public boolean match(FunctionName functionName, List<RelDataType> paramTypeList) {
    if (!functionName.equals(this.functionName)) return false;
    // For complex type checkers (e.g., OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED),
    // the typeChecker will be null because only simple family-based type checks are currently
    // supported.
    if (typeChecker == null) return true;
    return typeChecker.checkOperandTypes(paramTypeList);
  }
}
