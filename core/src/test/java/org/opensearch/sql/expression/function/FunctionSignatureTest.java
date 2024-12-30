/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.expression.function.FunctionSignature.EXACTLY_MATCH;
import static org.opensearch.sql.expression.function.FunctionSignature.NOT_MATCH;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FunctionSignatureTest {
  @Mock private FunctionSignature funcSignature;
  @Mock private List<ExprType> funcParamTypeList;

  private final FunctionName unresolvedFuncName = FunctionName.of("add");
  private final List<ExprType> unresolvedParamTypeList =
      Arrays.asList(ExprCoreType.INTEGER, ExprCoreType.FLOAT);

  @Test
  void signature_name_not_match() {
    when(funcSignature.getFunctionName()).thenReturn(FunctionName.of(("diff")));
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(this.unresolvedFuncName, unresolvedParamTypeList);

    assertEquals(NOT_MATCH, unresolvedFunSig.match(funcSignature));
  }

  @Test
  void signature_arguments_size_not_match() {
    when(funcSignature.getFunctionName()).thenReturn(unresolvedFuncName);
    when(funcSignature.getParamTypeList()).thenReturn(funcParamTypeList);
    when(funcParamTypeList.size()).thenReturn(1);
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(unresolvedFuncName, unresolvedParamTypeList);

    assertEquals(NOT_MATCH, unresolvedFunSig.match(funcSignature));
  }

  @Test
  void signature_exactly_match() {
    when(funcSignature.getFunctionName()).thenReturn(unresolvedFuncName);
    when(funcSignature.getParamTypeList()).thenReturn(unresolvedParamTypeList);
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(unresolvedFuncName, unresolvedParamTypeList);

    assertEquals(EXACTLY_MATCH, unresolvedFunSig.match(funcSignature));
  }

  @Test
  void signature_not_match() {
    when(funcSignature.getFunctionName()).thenReturn(unresolvedFuncName);
    when(funcSignature.getParamTypeList())
        .thenReturn(Arrays.asList(ExprCoreType.STRING, ExprCoreType.STRING));
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(unresolvedFuncName, unresolvedParamTypeList);

    assertEquals(NOT_MATCH, unresolvedFunSig.match(funcSignature));
  }

  @Test
  void signature_widening_match() {
    when(funcSignature.getFunctionName()).thenReturn(unresolvedFuncName);
    when(funcSignature.getParamTypeList())
        .thenReturn(Arrays.asList(ExprCoreType.FLOAT, ExprCoreType.FLOAT));
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(unresolvedFuncName, unresolvedParamTypeList);

    assertEquals(2, unresolvedFunSig.match(funcSignature));
  }

  @Test
  void format_types() {
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(unresolvedFuncName, unresolvedParamTypeList);

    assertEquals("[INTEGER,FLOAT]", unresolvedFunSig.formatTypes());
  }
}
