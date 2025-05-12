/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/** Function signature is composed by function name and arguments list. */
public record CalciteFuncSignature(FunctionName functionName, PPLTypeChecker typeChecker) {

  public boolean match(FunctionName functionName, List<RelDataType> paramTypeList) {
    if (!functionName.equals(this.functionName())) return false;
    if (typeChecker == null) return true;
    return typeChecker.checkOperandTypes(paramTypeList);
  }
}
