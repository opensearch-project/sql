/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.window.patterns.BufferPatternWindowFunction;
import org.opensearch.sql.expression.window.ranking.DenseRankFunction;
import org.opensearch.sql.expression.window.ranking.RankFunction;
import org.opensearch.sql.expression.window.ranking.RankingWindowFunction;
import org.opensearch.sql.expression.window.ranking.RowNumberFunction;

/** Window functions that register all window functions in function repository. */
@UtilityClass
public class WindowFunctions {

  /**
   * Register all window functions to function repository.
   *
   * @param repository function repository
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(rowNumber());
    repository.register(rank());
    repository.register(denseRank());
    repository.register(brain());
  }

  private DefaultFunctionResolver rowNumber() {
    return rankingFunction(BuiltinFunctionName.ROW_NUMBER.getName(), RowNumberFunction::new);
  }

  private DefaultFunctionResolver rank() {
    return rankingFunction(BuiltinFunctionName.RANK.getName(), RankFunction::new);
  }

  private DefaultFunctionResolver denseRank() {
    return rankingFunction(BuiltinFunctionName.DENSE_RANK.getName(), DenseRankFunction::new);
  }

  private DefaultFunctionResolver brain() {
    FunctionName functionName = BuiltinFunctionName.BRAIN.getName();
    FunctionBuilder functionBuilder =
        (functionProperties, arguments) -> new BufferPatternWindowFunction(arguments);
    return new DefaultFunctionResolver(
        functionName,
        ImmutableMap.of(
            new FunctionSignature(functionName, ImmutableList.of(STRING)), functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, STRING)), functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, INTEGER)), functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, DOUBLE)), functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, STRING, INTEGER)),
                functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, STRING, DOUBLE)),
                functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, INTEGER, DOUBLE)),
                functionBuilder,
            new FunctionSignature(functionName, ImmutableList.of(STRING, STRING, INTEGER, DOUBLE)),
                functionBuilder));
  }

  private DefaultFunctionResolver rankingFunction(
      FunctionName functionName, Supplier<RankingWindowFunction> constructor) {
    FunctionSignature functionSignature = new FunctionSignature(functionName, emptyList());
    FunctionBuilder functionBuilder = (functionProperties, arguments) -> constructor.get();
    return new DefaultFunctionResolver(
        functionName, ImmutableMap.of(functionSignature, functionBuilder));
  }
}
