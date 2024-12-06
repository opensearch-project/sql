/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static java.util.Collections.singletonList;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
 * The definition of aggregator functions <em>avg</em>, <em>sum</em>, <em>min</em>, <em>max</em> and
 * <em>count</em>.<br>
 * All of them accept a list of numbers and produce a number. <em>avg</em>, <em>min</em> and
 * <em>max</em> also accept datetime types.<br>
 * <em>count</em> accepts values of all types.
 */
@UtilityClass
public class AggregatorFunctions {
  /**
   * Register Aggregation Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(avg());
    repository.register(sum());
    repository.register(count());
    repository.register(min());
    repository.register(max());
    repository.register(varSamp());
    repository.register(varPop());
    repository.register(stddevSamp());
    repository.register(stddevPop());
    repository.register(take());
    repository.register(percentileApprox());
  }

  private static DefaultFunctionResolver avg() {
    FunctionName functionName = BuiltinFunctionName.AVG.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> new AvgAggregator(arguments, DOUBLE))
            .put(
                new FunctionSignature(functionName, singletonList(DATE)),
                (functionProperties, arguments) -> new AvgAggregator(arguments, DATE))
            .put(
                new FunctionSignature(functionName, singletonList(TIME)),
                (functionProperties, arguments) -> new AvgAggregator(arguments, TIME))
            .put(
                new FunctionSignature(functionName, singletonList(TIMESTAMP)),
                (functionProperties, arguments) -> new AvgAggregator(arguments, TIMESTAMP))
            .build());
  }

  private static DefaultFunctionResolver count() {
    FunctionName functionName = BuiltinFunctionName.COUNT.getName();
    DefaultFunctionResolver functionResolver =
        new DefaultFunctionResolver(
            functionName,
            ExprCoreType.coreTypes().stream()
                .collect(
                    Collectors.toMap(
                        type -> new FunctionSignature(functionName, singletonList(type)),
                        type ->
                            (functionProperties, arguments) ->
                                new CountAggregator(arguments, INTEGER))));
    return functionResolver;
  }

  private static DefaultFunctionResolver sum() {
    FunctionName functionName = BuiltinFunctionName.SUM.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(INTEGER)),
                (functionProperties, arguments) -> new SumAggregator(arguments, INTEGER))
            .put(
                new FunctionSignature(functionName, singletonList(LONG)),
                (functionProperties, arguments) -> new SumAggregator(arguments, LONG))
            .put(
                new FunctionSignature(functionName, singletonList(FLOAT)),
                (functionProperties, arguments) -> new SumAggregator(arguments, FLOAT))
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> new SumAggregator(arguments, DOUBLE))
            .build());
  }

  private static DefaultFunctionResolver min() {
    FunctionName functionName = BuiltinFunctionName.MIN.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(INTEGER)),
                (functionProperties, arguments) -> new MinAggregator(arguments, INTEGER))
            .put(
                new FunctionSignature(functionName, singletonList(LONG)),
                (functionProperties, arguments) -> new MinAggregator(arguments, LONG))
            .put(
                new FunctionSignature(functionName, singletonList(FLOAT)),
                (functionProperties, arguments) -> new MinAggregator(arguments, FLOAT))
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> new MinAggregator(arguments, DOUBLE))
            .put(
                new FunctionSignature(functionName, singletonList(STRING)),
                (functionProperties, arguments) -> new MinAggregator(arguments, STRING))
            .put(
                new FunctionSignature(functionName, singletonList(DATE)),
                (functionProperties, arguments) -> new MinAggregator(arguments, DATE))
            .put(
                new FunctionSignature(functionName, singletonList(TIME)),
                (functionProperties, arguments) -> new MinAggregator(arguments, TIME))
            .put(
                new FunctionSignature(functionName, singletonList(TIMESTAMP)),
                (functionProperties, arguments) -> new MinAggregator(arguments, TIMESTAMP))
            .build());
  }

  private static DefaultFunctionResolver max() {
    FunctionName functionName = BuiltinFunctionName.MAX.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(INTEGER)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, INTEGER))
            .put(
                new FunctionSignature(functionName, singletonList(LONG)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, LONG))
            .put(
                new FunctionSignature(functionName, singletonList(FLOAT)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, FLOAT))
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, DOUBLE))
            .put(
                new FunctionSignature(functionName, singletonList(STRING)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, STRING))
            .put(
                new FunctionSignature(functionName, singletonList(DATE)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, DATE))
            .put(
                new FunctionSignature(functionName, singletonList(TIME)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, TIME))
            .put(
                new FunctionSignature(functionName, singletonList(TIMESTAMP)),
                (functionProperties, arguments) -> new MaxAggregator(arguments, TIMESTAMP))
            .build());
  }

  private static DefaultFunctionResolver varSamp() {
    FunctionName functionName = BuiltinFunctionName.VARSAMP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> varianceSample(arguments, DOUBLE))
            .build());
  }

  private static DefaultFunctionResolver varPop() {
    FunctionName functionName = BuiltinFunctionName.VARPOP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> variancePopulation(arguments, DOUBLE))
            .build());
  }

  private static DefaultFunctionResolver stddevSamp() {
    FunctionName functionName = BuiltinFunctionName.STDDEV_SAMP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> stddevSample(arguments, DOUBLE))
            .build());
  }

  private static DefaultFunctionResolver stddevPop() {
    FunctionName functionName = BuiltinFunctionName.STDDEV_POP.getName();
    return new DefaultFunctionResolver(
        functionName,
        new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
            .put(
                new FunctionSignature(functionName, singletonList(DOUBLE)),
                (functionProperties, arguments) -> stddevPopulation(arguments, DOUBLE))
            .build());
  }

  private static DefaultFunctionResolver take() {
    FunctionName functionName = BuiltinFunctionName.TAKE.getName();
    DefaultFunctionResolver functionResolver =
        new DefaultFunctionResolver(
            functionName,
            new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(STRING, INTEGER)),
                    (functionProperties, arguments) -> new TakeAggregator(arguments, ARRAY))
                .build());
    return functionResolver;
  }

  private static DefaultFunctionResolver percentileApprox() {
    FunctionName functionName = BuiltinFunctionName.PERCENTILE_APPROX.getName();
    DefaultFunctionResolver functionResolver =
        new DefaultFunctionResolver(
            functionName,
            new ImmutableMap.Builder<FunctionSignature, FunctionBuilder>()
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(INTEGER, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, INTEGER))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(INTEGER, DOUBLE, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, INTEGER))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(LONG, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, LONG))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(LONG, DOUBLE, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, LONG))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(FLOAT, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, FLOAT))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(FLOAT, DOUBLE, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, FLOAT))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(DOUBLE, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, DOUBLE))
                .put(
                    new FunctionSignature(functionName, ImmutableList.of(DOUBLE, DOUBLE, DOUBLE)),
                    (functionProperties, arguments) ->
                        PercentileApproximateAggregator.percentileApprox(arguments, DOUBLE))
                .build());
    return functionResolver;
  }
}
