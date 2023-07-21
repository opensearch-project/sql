/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.expression.function.FunctionDSL.define;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.Expression;

@ExtendWith(MockitoExtension.class)
class FunctionDSLDefineTest extends FunctionDSLTestBase {

  @Test
  void define_variableArgs_test() {
    Pair<FunctionSignature, FunctionBuilder> resolvedA =
        Pair.of(SAMPLE_SIGNATURE_A, new SampleFunctionBuilder());

    FunctionResolver resolver = define(SAMPLE_NAME, v -> resolvedA);

    assertEquals(resolvedA, resolver.resolve(SAMPLE_SIGNATURE_A));
  }

  @Test
  void define_test() {
    Pair<FunctionSignature, FunctionBuilder> resolved =
        Pair.of(SAMPLE_SIGNATURE_A, new SampleFunctionBuilder());

    FunctionResolver resolver = define(SAMPLE_NAME, List.of(v -> resolved));

    assertEquals(resolved, resolver.resolve(SAMPLE_SIGNATURE_A));
  }

  @Test
  void define_name_test() {
    Pair<FunctionSignature, FunctionBuilder> resolved =
        Pair.of(SAMPLE_SIGNATURE_A, new SampleFunctionBuilder());

    FunctionResolver resolver = define(SAMPLE_NAME, List.of(v -> resolved));

    assertEquals(SAMPLE_NAME, resolver.getFunctionName());
  }

  static class SampleFunctionBuilder implements FunctionBuilder {
    @Override
    public FunctionImplementation apply(FunctionProperties functionProperties,
                                        List<Expression> arguments) {
      return new SampleFunctionImplementation(arguments);
    }
  }

  @RequiredArgsConstructor
  static class SampleFunctionImplementation implements FunctionImplementation {
    @Getter
    private final List<Expression> arguments;

    @Override
    public FunctionName getFunctionName() {
      return SAMPLE_NAME;
    }
  }
}
