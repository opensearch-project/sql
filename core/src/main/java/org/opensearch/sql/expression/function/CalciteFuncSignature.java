/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.expression.function.PPLFuncImpTable.FunctionImp.ANY_TYPE;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/** Function signature is composed by function name and arguments list. */
public class CalciteFuncSignature {

  private final FunctionName functionName;
  private final List<RelDataType> funcArgTypes;

  public CalciteFuncSignature(FunctionName functionName, List<RelDataType> funcArgTypes) {
    this.functionName = functionName;
    this.funcArgTypes = funcArgTypes;
  }

  public FunctionName getFunctionName() {
    return functionName;
  }

  public List<RelDataType> getFuncArgTypes() {
    return funcArgTypes;
  }

  public boolean match(FunctionName functionName, List<RelDataType> paramTypeList) {
    if (funcArgTypes == null) return true;
    if (!functionName.equals(this.functionName) || paramTypeList.size() != funcArgTypes.size()) {
      return false;
    }
    for (int i = 0; i < paramTypeList.size(); i++) {
      RelDataType paramType = paramTypeList.get(i);
      RelDataType funcType = funcArgTypes.get(i);
      if (ANY_TYPE != funcType && paramType.getFamily() != funcType.getFamily()) {
        return false;
      }
    }
    return true;
  }
}
