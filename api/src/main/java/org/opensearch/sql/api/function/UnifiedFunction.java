/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.io.Serializable;
import java.util.List;

/**
 * A unified function abstraction that provides an engine-agnostic way to represent and evaluate
 * functions, enabling functions to be implemented once and used across multiple execution engines
 * without engine-specific code duplication.
 *
 * <p>Types are represented as SQL type name strings (e.g., "VARCHAR", "INTEGER", "ARRAY&lt;T&gt;",
 * "STRUCT&lt;...&gt;") for engine-agnostic serialization.
 *
 * @see java.io.Serializable
 */
public interface UnifiedFunction extends Serializable {

  /**
   * Returns the name of the function.
   *
   * @return the function name.
   */
  String getFunctionName();

  /**
   * Returns the unified type names expected for the input arguments.
   *
   * @return list of SQL type names for input arguments.
   */
  List<String> getInputTypes();

  /**
   * Returns the unified type name of the function result.
   *
   * @return unified type name of the function result.
   */
  String getReturnType();

  /**
   * Evaluates the function with the provided input values.
   *
   * @param inputs argument values evaluated by the caller.
   * @return the evaluated result, may be null depending on the function implementation.
   */
  Object eval(List<Object> inputs);
}
