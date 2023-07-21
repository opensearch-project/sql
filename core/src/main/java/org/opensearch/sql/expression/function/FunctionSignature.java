/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;

import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.WideningTypeRule;

/**
 * Function signature is composed by function name and arguments list.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
public class FunctionSignature {
  public static final Integer NOT_MATCH = Integer.MAX_VALUE;
  public static final Integer EXACTLY_MATCH = 0;

  private final FunctionName functionName;
  private final List<ExprType> paramTypeList;

  /**
   * calculate the function signature match degree.
   *
   * @return  EXACTLY_MATCH: exactly match
   *          NOT_MATCH: not match
   *          By widening rule, the small number means better match
   */
  public int match(FunctionSignature functionSignature) {
    List<ExprType> functionTypeList = functionSignature.getParamTypeList();
    if (!functionName.equals(functionSignature.getFunctionName())
        || paramTypeList.size() != functionTypeList.size()) {
      return NOT_MATCH;
    }
    // TODO: improve to support regular and array type mixed, ex. func(int,string,array)
    if (isVarArgFunction(functionTypeList)) {
      return EXACTLY_MATCH;
    }

    int matchDegree = EXACTLY_MATCH;
    for (int i = 0; i < paramTypeList.size(); i++) {
      ExprType paramType = paramTypeList.get(i);
      ExprType funcType = functionTypeList.get(i);
      int match = WideningTypeRule.distance(paramType, funcType);
      if (match == WideningTypeRule.IMPOSSIBLE_WIDENING) {
        return NOT_MATCH;
      } else {
        matchDegree += match;
      }
    }
    return matchDegree;
  }

  /**
   * util function for formatted arguments list.
   */
  public String formatTypes() {
    return getParamTypeList().stream()
        .map(ExprType::typeName)
        .collect(Collectors.joining(",", "[", "]"));
  }

  /**
   * util function - returns true if function has variable arguments.
   */
  protected static boolean isVarArgFunction(List<ExprType> argTypes) {
    return argTypes.size() == 1 && argTypes.get(0) == ARRAY;
  }
}
