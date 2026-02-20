/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.DIGESTS;
import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.SOURCES;

import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.Source;

/**
 * DataContext implementations for scripted metric aggregation script phases. Provides access to
 * params, state/states variables, and document fields depending on the phase.
 *
 * <p>Each script phase has its own context:
 *
 * <ul>
 *   <li>{@link InitContext} - init_script: params and state
 *   <li>{@link MapContext} - map_script: params, state, doc values, and source lookup
 *   <li>{@link CombineContext} - combine_script: params and state
 *   <li>{@link ReduceContext} - reduce_script: params and states (array from all shards)
 * </ul>
 */
public abstract class ScriptedMetricDataContext implements DataContext {

  protected final Map<String, Object> params;
  protected final List<Source> sources;
  protected final List<Object> digests;

  protected ScriptedMetricDataContext(Map<String, Object> params) {
    this.params = params;
    this.sources = ((List<Integer>) params.get(SOURCES)).stream().map(Source::fromValue).toList();
    this.digests = (List<Object>) params.get(DIGESTS);
  }

  @Override
  public @Nullable SchemaPlus getRootSchema() {
    return null;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return null;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return null;
  }

  /**
   * Merges the execution result into the state map. This is a common operation used in init_script
   * and map_script phases to update the accumulator state.
   *
   * <p>If the result is a Map, its entries are merged into the state. Otherwise, the result is
   * stored under the "accumulator" key.
   *
   * @param result The result array from function execution (may be null or empty)
   * @param state The state map to update
   */
  @SuppressWarnings("unchecked")
  public static void mergeResultIntoState(Object[] result, Map<String, Object> state) {
    if (result != null && result.length > 0) {
      if (result[0] instanceof Map) {
        state.putAll((Map<String, Object>) result[0]);
      } else {
        state.put("accumulator", result[0]);
      }
    }
  }

  /**
   * Parse dynamic parameter index from name pattern "?N".
   *
   * @param name The parameter name (expected format: "?0", "?1", etc.)
   * @return The parameter index
   * @throws IllegalArgumentException if name doesn't match expected pattern or is malformed
   */
  protected int parseDynamicParamIndex(String name) {
    if (!name.startsWith("?")) {
      throw new IllegalArgumentException(
          "Unexpected parameter name format: " + name + ". Expected '?N' pattern.");
    }
    int index;
    try {
      index = Integer.parseInt(name.substring(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Malformed parameter name '" + name + "'. Expected '?N' pattern where N is an integer.",
          e);
    }
    if (index >= sources.size()) {
      throw new IllegalArgumentException(
          "Parameter index " + index + " out of bounds. Sources size: " + sources.size());
    }
    return index;
  }

  /**
   * Base class for init and combine phases that share identical get() logic. Both phases only have
   * access to params and state (no doc values).
   */
  protected abstract static class StateOnlyContext extends ScriptedMetricDataContext {
    protected final Map<String, Object> state;

    protected StateOnlyContext(Map<String, Object> params, Map<String, Object> state) {
      super(params);
      this.state = state;
    }

    @Override
    public Object get(String name) {
      int index = parseDynamicParamIndex(name);
      return switch (sources.get(index)) {
        case SPECIAL_VARIABLE -> state;
        case LITERAL -> digests.get(index);
        default ->
            throw new IllegalStateException(
                "Unexpected source type " + sources.get(index) + " in StateOnlyContext");
      };
    }
  }

  /** DataContext for InitScript phase - provides params and state. */
  public static class InitContext extends StateOnlyContext {
    public InitContext(Map<String, Object> params, Map<String, Object> state) {
      super(params, state);
    }
  }

  /** DataContext for CombineScript phase - provides params and state. */
  public static class CombineContext extends StateOnlyContext {
    public CombineContext(Map<String, Object> params, Map<String, Object> state) {
      super(params, state);
    }
  }

  /** DataContext for MapScript phase - provides params, state, doc values, and source lookup. */
  public static class MapContext extends ScriptedMetricDataContext {
    private final Map<String, Object> state;
    private final Map<String, ScriptDocValues<?>> doc;
    private final SourceLookup sourceLookup;

    public MapContext(
        Map<String, Object> params,
        Map<String, Object> state,
        Map<String, ScriptDocValues<?>> doc,
        SourceLookup sourceLookup) {
      super(params);
      this.state = state;
      this.doc = doc;
      this.sourceLookup = sourceLookup;
    }

    @Override
    public Object get(String name) {
      int index = parseDynamicParamIndex(name);
      return switch (sources.get(index)) {
        case SPECIAL_VARIABLE -> state;
        case LITERAL -> digests.get(index);
        case DOC_VALUE -> getDocValue((String) digests.get(index));
        case SOURCE -> sourceLookup != null ? sourceLookup.get((String) digests.get(index)) : null;
      };
    }

    private Object getDocValue(String fieldName) {
      if (doc != null && doc.containsKey(fieldName)) {
        ScriptDocValues<?> docValue = doc.get(fieldName);
        if (docValue != null && !docValue.isEmpty()) {
          return docValue.get(0);
        }
      }
      return null;
    }
  }

  /** DataContext for ReduceScript phase - provides params and states array from all shards. */
  public static class ReduceContext extends ScriptedMetricDataContext {
    private final List<Object> states;

    public ReduceContext(Map<String, Object> params, List<Object> states) {
      super(params);
      this.states = states;
    }

    @Override
    public Object get(String name) {
      int index = parseDynamicParamIndex(name);
      return switch (sources.get(index)) {
        case SPECIAL_VARIABLE -> states;
        case LITERAL -> digests.get(index);
        default ->
            throw new IllegalStateException(
                "Unexpected source type " + sources.get(index) + " in ReduceContext");
      };
    }
  }
}
