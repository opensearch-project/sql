/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.io.Serializable;
import java.util.List;

/**
 * A unified abstraction over execution engines that can evaluate functions using a common contract
 * across different engines (Spark, Calcite, Flink, etc.).
 *
 * <p>This interface provides an engine-agnostic way to represent and evaluate functions, enabling
 * functions to be implemented once and used across multiple execution engines without
 * engine-specific code duplication.
 *
 * <p>The interface is designed to be serializable, allowing function instances to be distributed
 * across execution engines in distributed computing environments.
 *
 * <h2>Type System</h2>
 *
 * <p>Types are represented as SQL type name strings for engine-agnostic serialization:
 *
 * <ul>
 *   <li>Primitive types: "VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"
 *   <li>Array types: "ARRAY&lt;ELEMENT_TYPE&gt;" (e.g., "ARRAY&lt;INTEGER&gt;")
 *   <li>Struct types: "STRUCT&lt;field1:TYPE1, field2:TYPE2&gt;" (e.g., "STRUCT&lt;name:VARCHAR,
 *       age:INTEGER&gt;")
 *   <li>Unknown types: "UNKNOWN"
 * </ul>
 *
 * <h2>Evaluation Contract</h2>
 *
 * <p>The {@link #eval(List)} method accepts already-evaluated argument values and returns the
 * computed result. Implementations should:
 *
 * <ul>
 *   <li>Validate that input values match the declared input types
 *   <li>Handle null inputs appropriately
 *   <li>Return results that match the declared return type
 *   <li>Throw appropriate exceptions for invalid inputs or evaluation errors
 * </ul>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * UnifiedFunction upperFunc = // ... obtain function instance
 *
 * // Get function metadata
 * String name = upperFunc.getFunctionName();  // "UPPER"
 * List<String> inputTypes = upperFunc.getInputTypes();  // ["VARCHAR"]
 * String returnType = upperFunc.getReturnType();  // "VARCHAR"
 *
 * // Evaluate function
 * Object result = upperFunc.eval(Arrays.asList("hello"));  // "HELLO"
 * }</pre>
 *
 * @see java.io.Serializable
 */
public interface UnifiedFunction extends Serializable {

  /**
   * Returns the name of the function.
   *
   * <p>Function names should be consistent across all execution engines and typically follow SQL
   * naming conventions (e.g., "CONCAT", "UPPER", "ABS").
   *
   * @return the function name, never null
   */
  String getFunctionName();

  /**
   * Returns the SQL type names expected for the input arguments.
   *
   * <p>Type names are represented as strings for engine-agnostic serialization. The list order
   * corresponds to the argument positions in {@link #eval(List)}.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Single string argument: ["VARCHAR"]
   *   <li>Two numeric arguments: ["INTEGER", "INTEGER"]
   *   <li>Array argument: ["ARRAY&lt;DOUBLE&gt;"]
   *   <li>Struct argument: ["STRUCT&lt;x:DOUBLE, y:DOUBLE&gt;"]
   * </ul>
   *
   * @return list of SQL type names for input arguments, never null
   */
  List<String> getInputTypes();

  /**
   * Returns the SQL type name of the function result.
   *
   * <p>The return type is represented as a string for engine-agnostic serialization.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>String result: "VARCHAR"
   *   <li>Numeric result: "DOUBLE"
   *   <li>Array result: "ARRAY&lt;INTEGER&gt;"
   *   <li>Struct result: "STRUCT&lt;sum:DOUBLE, count:INTEGER&gt;"
   * </ul>
   *
   * @return SQL type name of the function result, never null
   */
  String getReturnType();

  /**
   * Evaluates the function with the provided input values.
   *
   * <p>The inputs are already-evaluated argument values provided by the caller. The number and
   * types of inputs should match the types declared by {@link #getInputTypes()}.
   *
   * <p>Implementations should:
   *
   * <ul>
   *   <li>Validate input count matches {@link #getInputTypes()} size
   *   <li>Validate input types match declared types (or can be coerced)
   *   <li>Handle null inputs appropriately
   *   <li>Return a result matching the declared {@link #getReturnType()}
   * </ul>
   *
   * <p>Example:
   *
   * <pre>{@code
   * // CONCAT function with two string inputs
   * UnifiedFunction concat = // ... obtain function
   * Object result = concat.eval(Arrays.asList("Hello", "World"));  // "HelloWorld"
   * }</pre>
   *
   * @param inputs argument values evaluated by the caller, never null but may contain null elements
   * @return the evaluated result, may be null depending on the function implementation
   * @throws IllegalArgumentException if input count or types don't match expectations
   * @throws RuntimeException if evaluation fails for any other reason
   */
  Object eval(List<Object> inputs);
}
