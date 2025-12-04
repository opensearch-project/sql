/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

/** Function signature is composed by function name and arguments list. */
public record CalciteFuncSignature(FunctionName functionName, SqlOperandTypeChecker typeChecker) {

  // TODO: Refactor this match method
  public boolean match(FunctionName functionName, List<RelDataType> argTypes) {
    return functionName.equals(this.functionName());
  }
}
