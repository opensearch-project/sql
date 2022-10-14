/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.aggregation.StdDevAggregator.stddevPopulation;
import static org.opensearch.sql.expression.aggregation.StdDevAggregator.stddevSample;
import static org.opensearch.sql.expression.aggregation.VarianceAggregator.variancePopulation;
import static org.opensearch.sql.expression.aggregation.VarianceAggregator.varianceSample;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;

/**
 * The definition of aggregator function
 * avg, Accepts two numbers and produces a number.
 * sum, Accepts two numbers and produces a number.
 * max, Accepts two numbers and produces a number.
 * min, Accepts two numbers and produces a number.
 * count, Accepts two numbers and produces a number.
 */
@UtilityClass
public class AggregatorFunction {
  /**
   * Register Aggregation Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(take());
    repository.register(avg());
    repository.register(sum());
    repository.register(count());
    repository.register(min());
    repository.register(max());
    repository.register(varSamp());
    repository.register(varPop());
    repository.register(stddevSamp());
    repository.register(stddevPop());
  }

  private static DefaultFunctionResolver take() {
    FunctionName functionName = BuiltinFunctionName.TAKE.getName();
    DefaultFunctionResolver functionResolver = new DefaultFunctionResolver(functionName,
        ExprCoreType.coreTypes().stream().collect(Collectors.toMap(
            type -> new FunctionSignature(functionName, Collections.singletonList(type)),
            type -> arguments -> new TakeAggregator(arguments, type))));
    return functionResolver;
  }

  private static DefaultFunctionResolver avg() {
    FunctionName functionName = BuiltinFunctionName.AVG.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> new AvgAggregator(arguments, DOUBLE))
            .build()
    );
  }

  private static DefaultFunctionResolver count() {
    FunctionName functionName = BuiltinFunctionName.COUNT.getName();
    DefaultFunctionResolver functionResolver = new DefaultFunctionResolver(functionName,
        ExprCoreType.coreTypes().stream().collect(Collectors.toMap(
          type -> new FunctionSignature(functionName, Collections.singletonList(type)),
          type -> arguments -> new CountAggregator(arguments, INTEGER))));
    return functionResolver;
  }

  private static DefaultFunctionResolver sum() {
    FunctionName functionName = BuiltinFunctionName.SUM.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(INTEGER)),
                arguments -> new SumAggregator(arguments, INTEGER))
            .put(new FunctionSignature(functionName, Collections.singletonList(LONG)),
                arguments -> new SumAggregator(arguments, LONG))
            .put(new FunctionSignature(functionName, Collections.singletonList(FLOAT)),
                arguments -> new SumAggregator(arguments, FLOAT))
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> new SumAggregator(arguments, DOUBLE))
            .build()
    );
  }

  private static DefaultFunctionResolver min() {
    FunctionName functionName = BuiltinFunctionName.MIN.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(INTEGER)),
                arguments -> new MinAggregator(arguments, INTEGER))
            .put(new FunctionSignature(functionName, Collections.singletonList(LONG)),
                arguments -> new MinAggregator(arguments, LONG))
            .put(new FunctionSignature(functionName, Collections.singletonList(FLOAT)),
                arguments -> new MinAggregator(arguments, FLOAT))
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> new MinAggregator(arguments, DOUBLE))
            .put(new FunctionSignature(functionName, Collections.singletonList(STRING)),
                arguments -> new MinAggregator(arguments, STRING))
            .put(new FunctionSignature(functionName, Collections.singletonList(DATE)),
                arguments -> new MinAggregator(arguments, DATE))
            .put(new FunctionSignature(functionName, Collections.singletonList(DATETIME)),
                arguments -> new MinAggregator(arguments, DATETIME))
            .put(new FunctionSignature(functionName, Collections.singletonList(TIME)),
                arguments -> new MinAggregator(arguments, TIME))
            .put(new FunctionSignature(functionName, Collections.singletonList(TIMESTAMP)),
                arguments -> new MinAggregator(arguments, TIMESTAMP))
            .build());
  }

  private static DefaultFunctionResolver max() {
    FunctionName functionName = BuiltinFunctionName.MAX.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(INTEGER)),
                arguments -> new MaxAggregator(arguments, INTEGER))
            .put(new FunctionSignature(functionName, Collections.singletonList(LONG)),
                arguments -> new MaxAggregator(arguments, LONG))
            .put(new FunctionSignature(functionName, Collections.singletonList(FLOAT)),
                arguments -> new MaxAggregator(arguments, FLOAT))
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> new MaxAggregator(arguments, DOUBLE))
            .put(new FunctionSignature(functionName, Collections.singletonList(STRING)),
                arguments -> new MaxAggregator(arguments, STRING))
            .put(new FunctionSignature(functionName, Collections.singletonList(DATE)),
                arguments -> new MaxAggregator(arguments, DATE))
            .put(new FunctionSignature(functionName, Collections.singletonList(DATETIME)),
                arguments -> new MaxAggregator(arguments, DATETIME))
            .put(new FunctionSignature(functionName, Collections.singletonList(TIME)),
                arguments -> new MaxAggregator(arguments, TIME))
            .put(new FunctionSignature(functionName, Collections.singletonList(TIMESTAMP)),
                arguments -> new MaxAggregator(arguments, TIMESTAMP))
            .build()
    );
  }

  private static DefaultFunctionResolver varSamp() {
    FunctionName functionName = BuiltinFunctionName.VARSAMP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> varianceSample(arguments, DOUBLE))
            .build()
    );
  }

  private static DefaultFunctionResolver varPop() {
    FunctionName functionName = BuiltinFunctionName.VARPOP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> variancePopulation(arguments, DOUBLE))
            .build()
    );
  }

  private static DefaultFunctionResolver stddevSamp() {
    FunctionName functionName = BuiltinFunctionName.STDDEV_SAMP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> stddevSample(arguments, DOUBLE))
            .build()
    );
  }

  private static DefaultFunctionResolver stddevPop() {
    FunctionName functionName = BuiltinFunctionName.STDDEV_POP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(new FunctionSignature(functionName, Collections.singletonList(DOUBLE)),
                arguments -> stddevPopulation(arguments, DOUBLE))
            .build()
    );
  }
}
