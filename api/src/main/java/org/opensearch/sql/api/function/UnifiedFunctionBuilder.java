/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.io.Serializable;
import java.util.List;

/**
 * Builder for creating {@link UnifiedFunction} instances with specific input types.
 *
 * <p>This abstraction bridges PPL function definitions to external execution engines (Spark, Flink,
 * etc.) by providing a factory method that constructs engine-agnostic function instances.
 */
@FunctionalInterface
public interface UnifiedFunctionBuilder extends Serializable {

  /**
   * Builds a {@link UnifiedFunction} instance for the specified input types.
   *
   * @param inputTypes SQL type names for function arguments (e.g., ["VARCHAR", "INTEGER"])
   * @return a UnifiedFunction instance configured for the specified input types
   * @throws IllegalArgumentException if input types are invalid for this function
   */
  UnifiedFunction build(List<String> inputTypes);
}
