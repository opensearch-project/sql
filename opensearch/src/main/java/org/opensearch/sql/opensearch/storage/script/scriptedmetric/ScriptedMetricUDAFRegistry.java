/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.storage.script.scriptedmetric.udaf.PatternScriptedMetricUDAF;

/**
 * Registry for ScriptedMetricUDAF implementations.
 *
 * <p>This registry provides a lookup mechanism for finding UDAF implementations that can be pushed
 * down to OpenSearch as scripted metric aggregations. Each UDAF implementation is registered by its
 * function name.
 *
 * <p>To add a new UDAF pushdown:
 *
 * <ol>
 *   <li>Create a class implementing {@link ScriptedMetricUDAF}
 *   <li>Register it in this registry by calling {@link #register(ScriptedMetricUDAF)}
 * </ol>
 */
public final class ScriptedMetricUDAFRegistry {

  /** Singleton instance */
  public static final ScriptedMetricUDAFRegistry INSTANCE = new ScriptedMetricUDAFRegistry();

  private final Map<BuiltinFunctionName, ScriptedMetricUDAF> udafMap;

  private ScriptedMetricUDAFRegistry() {
    this.udafMap = new HashMap<>();
    registerBuiltinUDAFs();
  }

  /** Register all built-in scripted metric UDAFs. */
  private void registerBuiltinUDAFs() {
    // Register Pattern (BRAIN) UDAF
    register(PatternScriptedMetricUDAF.INSTANCE);
  }

  /**
   * Register a ScriptedMetricUDAF implementation.
   *
   * @param udaf The UDAF implementation to register
   */
  public void register(ScriptedMetricUDAF udaf) {
    udafMap.put(udaf.getFunctionName(), udaf);
  }

  /**
   * Look up a ScriptedMetricUDAF by function name.
   *
   * @param functionName The function name to look up
   * @return Optional containing the UDAF if found, empty otherwise
   */
  public Optional<ScriptedMetricUDAF> lookup(BuiltinFunctionName functionName) {
    return Optional.ofNullable(udafMap.get(functionName));
  }

  /**
   * Look up a ScriptedMetricUDAF by function name string.
   *
   * @param functionName The function name string to look up
   * @return Optional containing the UDAF if found, empty otherwise
   */
  public Optional<ScriptedMetricUDAF> lookup(String functionName) {
    return BuiltinFunctionName.ofAggregation(functionName)
        .flatMap(name -> Optional.ofNullable(udafMap.get(name)));
  }

  /**
   * Check if a function name has a registered ScriptedMetricUDAF.
   *
   * @param functionName The function name to check
   * @return true if a UDAF is registered for this function
   */
  public boolean hasUDAF(BuiltinFunctionName functionName) {
    return udafMap.containsKey(functionName);
  }
}
