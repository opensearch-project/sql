/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

class NestedFunctionResolverTest {
  private final FunctionName sampleFuncName = FunctionName.of("sample_function");
  private NestedFunctionResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new NestedFunctionResolver(sampleFuncName);
  }

  @Test
  void resolve_correct_name_test() {
    var sig = new FunctionSignature(sampleFuncName, List.of(STRING));
    Pair<FunctionSignature, FunctionBuilder> builderPair = resolver.resolve(sig);
    assertEquals(sampleFuncName, builderPair.getKey().getFunctionName());
  }

  @Test
  void resolve_invalid_name_test() {
    var wrongFuncName = FunctionName.of("wrong_func");
    var sig = new FunctionSignature(wrongFuncName, List.of(STRING));
    Exception exception = assertThrows(SemanticCheckException.class,
        () -> resolver.resolve(sig));
    assertEquals("Expected 'sample_function' but got 'wrong_func'",
        exception.getMessage());
  }
}
