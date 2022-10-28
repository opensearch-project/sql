/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class ExpressionConfigTest {
  private static AnnotationConfigApplicationContext createContext() {
    var context = new AnnotationConfigApplicationContext();
    context.register(ExpressionConfig.class);
    context.refresh();
    return context;
  }

  @Test
  void testContextIsFromBean() {
    AnnotationConfigApplicationContext context = createContext();
    BuiltinFunctionRepository repository = context.getBean(BuiltinFunctionRepository.class);
    assertEquals(repository.getFunctionProperties(),
        context.getBean(FunctionProperties.class));
  }

  @Test
  void testNotTheSame() {
    BuiltinFunctionRepository repositoryA
        = createContext().getBean(BuiltinFunctionRepository.class);
    BuiltinFunctionRepository repositoryB
        = createContext().getBean(BuiltinFunctionRepository.class);

    assertNotEquals(repositoryA.getFunctionProperties(),
        repositoryB.getFunctionProperties());

  }
}
