/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window;

import static java.util.Collections.emptyList;

import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
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

  private DefaultFunctionResolver rankingFunction(
      FunctionName functionName, Supplier<RankingWindowFunction> constructor) {
    FunctionSignature functionSignature = new FunctionSignature(functionName, emptyList());
    FunctionBuilder functionBuilder = (functionProperties, arguments) -> constructor.get();
    return new DefaultFunctionResolver(
        functionName, ImmutableMap.of(functionSignature, functionBuilder));
  }
}
