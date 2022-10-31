/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.config;

import java.time.Clock;
import java.util.HashMap;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.aggregation.AggregatorFunction;
import org.opensearch.sql.expression.datetime.DateTimeFunction;
import org.opensearch.sql.expression.datetime.IntervalClause;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.expression.operator.arthmetic.ArithmeticFunction;
import org.opensearch.sql.expression.operator.arthmetic.MathematicalFunction;
import org.opensearch.sql.expression.operator.convert.TypeCastOperator;
import org.opensearch.sql.expression.operator.predicate.BinaryPredicateOperator;
import org.opensearch.sql.expression.operator.predicate.UnaryPredicateOperator;
import org.opensearch.sql.expression.system.SystemFunctions;
import org.opensearch.sql.expression.text.TextFunction;
import org.opensearch.sql.expression.window.WindowFunctions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Expression Config for Spring IoC.
 */
@Configuration
public class ExpressionConfig {
  /**
   * BuiltinFunctionRepository constructor.
   */
  @Bean
  public BuiltinFunctionRepository functionRepository(FunctionProperties functionContext) {

    BuiltinFunctionRepository builtinFunctionRepository =
        new BuiltinFunctionRepository(new HashMap<>(), functionContext);
    ArithmeticFunction.register(builtinFunctionRepository);
    BinaryPredicateOperator.register(builtinFunctionRepository);
    MathematicalFunction.register(builtinFunctionRepository);
    UnaryPredicateOperator.register(builtinFunctionRepository);
    AggregatorFunction.register(builtinFunctionRepository);
    DateTimeFunction.register(builtinFunctionRepository);
    IntervalClause.register(builtinFunctionRepository);
    WindowFunctions.register(builtinFunctionRepository);
    TextFunction.register(builtinFunctionRepository);
    TypeCastOperator.register(builtinFunctionRepository);
    SystemFunctions.register(builtinFunctionRepository);
    OpenSearchFunctions.register(builtinFunctionRepository);
    return builtinFunctionRepository;
  }

  @Bean
  public FunctionProperties functionExecutionContext() {
    return new FunctionProperties(Clock.systemDefaultZone());
  }

  @Bean
  public DSL dsl(BuiltinFunctionRepository repository) {
    return new DSL(repository);
  }

}
