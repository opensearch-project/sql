/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.config;

import java.util.HashMap;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.aggregation.AggregatorFunction;
import org.opensearch.sql.expression.datetime.DateTimeFunction;
import org.opensearch.sql.expression.datetime.IntervalClause;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.operator.arthmetic.ArithmeticFunction;
import org.opensearch.sql.expression.operator.arthmetic.MathematicalFunction;
import org.opensearch.sql.expression.operator.convert.TypeCastOperator;
import org.opensearch.sql.expression.operator.predicate.BinaryPredicateOperator;
import org.opensearch.sql.expression.operator.predicate.UnaryPredicateOperator;
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
  public BuiltinFunctionRepository functionRepository() {
    BuiltinFunctionRepository builtinFunctionRepository =
        new BuiltinFunctionRepository(new HashMap<>());
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
    return builtinFunctionRepository;
  }

  @Bean
  public DSL dsl(BuiltinFunctionRepository repository) {
    return new DSL(repository);
  }
}
