/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * The read-only endpoint allow-list expressed as data: each allow-listed, read-only endpoint maps to its transport
 * action (a read-only call on {@link OpenSearchClient}), a fixed output schema (so the Calcite plan
 * can fix its row type at plan time), the query args it accepts, and a secret-field filter.
 *
 * <p>This is the single place the read-only allow-list and secret filtering are enforced.
 * Endpoints outside the registry, including every mutating endpoint, are rejected by {@link
 * #resolve} with a clear exception. Adding an endpoint is a reviewed change here, never arbitrary
 * pass-through.
 */
public final class RestEndpointRegistry {

  private RestEndpointRegistry() {}

  /** Produces the raw rows for an endpoint via a read-only client call. */
  @FunctionalInterface
  public interface RowFetcher {
    List<Map<String, Object>> fetch(OpenSearchClient client, RestSpec spec);
  }

  /** A single allow-listed endpoint description. */
  @Getter
  public static final class Endpoint {
    private final String path;
    private final LinkedHashMap<String, ExprType> schema;
    private final Set<String> allowedArgs;
    private final Set<String> secretFields;
    private final RowFetcher fetcher;

    Endpoint(
        String path,
        LinkedHashMap<String, ExprType> schema,
        Set<String> allowedArgs,
        Set<String> secretFields,
        RowFetcher fetcher) {
      this.path = path;
      this.schema = schema;
      this.allowedArgs = allowedArgs;
      this.secretFields = secretFields;
      this.fetcher = fetcher;
    }

    /** Dispatch the read-only call and shape the response into fixed-schema rows. */
    public List<ExprValue> toRows(OpenSearchClient client, RestSpec spec) {
      List<ExprValue> out = new ArrayList<>();
      for (Map<String, Object> raw : fetcher.fetch(client, spec)) {
        LinkedHashMap<String, ExprValue> tuple = new LinkedHashMap<>();
        for (Map.Entry<String, ExprType> col : schema.entrySet()) {
          if (secretFields.contains(col.getKey())) {
            // Never surface a secret-bearing field, even if the action returns it.
            continue;
          }
          tuple.put(col.getKey(), coerce(col.getKey(), col.getValue(), raw.get(col.getKey())));
        }
        out.add(new ExprTupleValue(tuple));
      }
      return out;
    }
  }

  private static final Map<String, Endpoint> REGISTRY = buildRegistry();

  private static Map<String, Endpoint> buildRegistry() {
    Map<String, Endpoint> m = new LinkedHashMap<>();

    // /_cluster/health — single-row cluster health snapshot (read-only monitor action).
    LinkedHashMap<String, ExprType> healthSchema = new LinkedHashMap<>();
    healthSchema.put("cluster_name", STRING);
    healthSchema.put("status", STRING);
    healthSchema.put("number_of_nodes", INTEGER);
    healthSchema.put("number_of_data_nodes", INTEGER);
    healthSchema.put("active_primary_shards", INTEGER);
    healthSchema.put("active_shards", INTEGER);
    healthSchema.put("relocating_shards", INTEGER);
    healthSchema.put("initializing_shards", INTEGER);
    healthSchema.put("unassigned_shards", INTEGER);
    healthSchema.put("timed_out", BOOLEAN);
    m.put(
        "/_cluster/health",
        new Endpoint(
            "/_cluster/health",
            healthSchema,
            Set.of("local"),
            Set.of(),
            (client, spec) -> List.of(client.clusterHealth(spec.getArgs()))));

    // /_cat/indices — one row per index (read-only monitor action).
    LinkedHashMap<String, ExprType> catSchema = new LinkedHashMap<>();
    catSchema.put("index", STRING);
    catSchema.put("health", STRING);
    catSchema.put("pri", INTEGER);
    catSchema.put("rep", INTEGER);
    catSchema.put("active_shards", INTEGER);
    m.put(
        "/_cat/indices",
        new Endpoint(
            "/_cat/indices",
            catSchema,
            Set.of("health"),
            Set.of(),
            (client, spec) -> client.catIndices(spec.getArgs())));

    // /_cat/nodes — one row per node with resource state (read-only monitor action).
    LinkedHashMap<String, ExprType> nodesSchema = new LinkedHashMap<>();
    nodesSchema.put("name", STRING);
    nodesSchema.put("ip", STRING);
    nodesSchema.put("node_role", STRING);
    nodesSchema.put("heap_percent", INTEGER);
    nodesSchema.put("ram_percent", INTEGER);
    nodesSchema.put("cpu", INTEGER);
    m.put(
        "/_cat/nodes",
        new Endpoint(
            "/_cat/nodes",
            nodesSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> client.catNodes(spec.getArgs())));

    // /_cat/cluster_manager — single row identifying the elected cluster manager node.
    LinkedHashMap<String, ExprType> clusterManagerSchema = new LinkedHashMap<>();
    clusterManagerSchema.put("id", STRING);
    clusterManagerSchema.put("host", STRING);
    clusterManagerSchema.put("ip", STRING);
    clusterManagerSchema.put("node", STRING);
    m.put(
        "/_cat/cluster_manager",
        new Endpoint(
            "/_cat/cluster_manager",
            clusterManagerSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> client.catClusterManager(spec.getArgs())));

    // /_cat/plugins — one row per installed plugin per node (read-only monitor action).
    LinkedHashMap<String, ExprType> pluginsSchema = new LinkedHashMap<>();
    pluginsSchema.put("name", STRING);
    pluginsSchema.put("component", STRING);
    pluginsSchema.put("version", STRING);
    m.put(
        "/_cat/plugins",
        new Endpoint(
            "/_cat/plugins",
            pluginsSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> client.catPlugins(spec.getArgs())));

    // /_cat/shards — one row per shard (read-only monitor action).
    LinkedHashMap<String, ExprType> shardsSchema = new LinkedHashMap<>();
    shardsSchema.put("index", STRING);
    shardsSchema.put("shard", INTEGER);
    shardsSchema.put("prirep", STRING);
    shardsSchema.put("state", STRING);
    shardsSchema.put("node", STRING);
    m.put(
        "/_cat/shards",
        new Endpoint(
            "/_cat/shards",
            shardsSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> client.catShards(spec.getArgs())));

    // /_cluster/state — single-row cluster-state epoch (version, uuid, manager node).
    LinkedHashMap<String, ExprType> stateSchema = new LinkedHashMap<>();
    stateSchema.put("cluster_name", STRING);
    stateSchema.put("state_uuid", STRING);
    stateSchema.put("version", LONG);
    stateSchema.put("cluster_manager_node", STRING);
    m.put(
        "/_cluster/state",
        new Endpoint(
            "/_cluster/state",
            stateSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> List.of(client.clusterState(spec.getArgs()))));

    // /_cluster/settings — one row per configured setting (persistent/transient tier).
    LinkedHashMap<String, ExprType> settingsSchema = new LinkedHashMap<>();
    settingsSchema.put("setting", STRING);
    settingsSchema.put("value", STRING);
    settingsSchema.put("tier", STRING);
    m.put(
        "/_cluster/settings",
        new Endpoint(
            "/_cluster/settings",
            settingsSchema,
            Set.of(),
            Set.of(),
            (client, spec) -> client.clusterSettings(spec.getArgs())));

    // /_resolve/index — one row per resolved index/alias/data_stream name.
    LinkedHashMap<String, ExprType> resolveSchema = new LinkedHashMap<>();
    resolveSchema.put("name", STRING);
    resolveSchema.put("type", STRING);
    m.put(
        "/_resolve/index",
        new Endpoint(
            "/_resolve/index",
            resolveSchema,
            Set.of("expand_wildcards"),
            Set.of(),
            (client, spec) -> client.resolveIndex(spec.getArgs())));

    return m;
  }

  /**
   * Resolve an allow-listed endpoint. Anything outside the registry (unknown path, mutating verb,
   * {@code /services/*}, plugin admin endpoints) is refused here.
   */
  public static Endpoint resolve(String path) {
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException(
          "rest endpoint must be a non-empty path. Supported read-only endpoints: "
              + REGISTRY.keySet());
    }
    Endpoint endpoint = REGISTRY.get(path);
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "rest endpoint ["
              + path
              + "] is not allow-listed. Only read-only in-cluster endpoints are supported: "
              + REGISTRY.keySet());
    }
    return endpoint;
  }

  /** Validate that every supplied query arg is accepted by the endpoint. */
  public static void validate(RestSpec spec) {
    Endpoint endpoint = resolve(spec.getEndpoint());
    if (spec.getCount() != null && spec.getCount() < 0) {
      throw new IllegalArgumentException(
          "rest endpoint ["
              + spec.getEndpoint()
              + "] count must be a non-negative integer, got ["
              + spec.getCount()
              + "]");
    }
    if (spec.getTimeout() != null) {
      // The timeout token is reserved in the grammar for forward compatibility, but a single
      // uniform timeout cannot map cleanly across the endpoints (wait-for-status vs
      // cluster-manager vs client socket timeouts differ per action). Reject it with a clear
      // client error rather than silently ignoring it.
      throw new IllegalArgumentException(
          "rest endpoint ["
              + spec.getEndpoint()
              + "] does not support the timeout argument yet");
    }
    if (spec.getArgs() != null) {
      for (String arg : spec.getArgs().keySet()) {
        if (!endpoint.getAllowedArgs().contains(arg)) {
          throw new IllegalArgumentException(
              "rest endpoint ["
                  + spec.getEndpoint()
                  + "] does not accept arg ["
                  + arg
                  + "]. Allowed args: "
                  + endpoint.getAllowedArgs());
        }
        validateArgValue(spec.getEndpoint(), arg, spec.getArgs().get(arg));
      }
    }
  }

  // Allowed value domains for the get-args that are applied server-side. Keys are validated against
  // the per-endpoint allow-list above; values are validated here so a user-supplied value is never
  // passed unchecked into an admin transport request.
  private static final Map<String, Set<String>> ARG_VALUE_DOMAINS =
      Map.of(
          "local", Set.of("true", "false"),
          "health", Set.of("green", "yellow", "red"));

  private static final Set<String> EXPAND_WILDCARDS_VALUES =
      Set.of("open", "closed", "hidden", "none", "all");

  /** Reject any get-arg value outside its allow-listed domain with a clear client error. */
  private static void validateArgValue(String endpoint, String arg, String value) {
    Set<String> domain = ARG_VALUE_DOMAINS.get(arg);
    if (domain != null) {
      if (value == null || !domain.contains(value.toLowerCase(java.util.Locale.ROOT))) {
        throw new IllegalArgumentException(
            "rest endpoint ["
                + endpoint
                + "] arg ["
                + arg
                + "] has an unsupported value ["
                + value
                + "]. Allowed values: "
                + domain);
      }
    } else if ("expand_wildcards".equals(arg)) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException(
            "rest endpoint ["
                + endpoint
                + "] arg [expand_wildcards] has an unsupported value ["
                + value
                + "]. Allowed values: "
                + EXPAND_WILDCARDS_VALUES);
      }
      for (String token : value.toLowerCase(java.util.Locale.ROOT).split(",")) {
        if (!EXPAND_WILDCARDS_VALUES.contains(token.trim())) {
          throw new IllegalArgumentException(
              "rest endpoint ["
                  + endpoint
                  + "] arg [expand_wildcards] has an unsupported value ["
                  + value
                  + "]. Allowed values: "
                  + EXPAND_WILDCARDS_VALUES);
        }
      }
    }
  }

  private static ExprValue coerce(String column, ExprType type, Object value) {
    if (value == null) {
      return ExprNullValue.of();
    }
    try {
      if (type == INTEGER) {
        return integerValue(toNumber(value).intValue());
      }
      if (type == LONG) {
        return longValue(toNumber(value).longValue());
      }
      if (type == DOUBLE) {
        return doubleValue(toNumber(value).doubleValue());
      }
      if (type == BOOLEAN) {
        return booleanValue(toBoolean(value));
      }
    } catch (RuntimeException e) {
      // Surface a clear client error (HTTP 400) instead of a raw ClassCastException /
      // NumberFormatException (HTTP 500) when an endpoint returns an unexpected value shape.
      throw new IllegalArgumentException(
          "rest endpoint value for column ["
              + column
              + "] could not be coerced to "
              + type
              + ": ["
              + value
              + "]");
    }
    return stringValue(String.valueOf(value));
  }

  /** Coerce a transport/JSON value to a Number, parsing numeric strings (e.g. the cat JSON API). */
  private static Number toNumber(Object value) {
    if (value instanceof Number n) {
      return n;
    }
    String s = String.valueOf(value).trim();
    if (s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0) {
      return Double.parseDouble(s);
    }
    return Long.parseLong(s);
  }

  /** Coerce a transport/JSON value to a boolean, accepting Boolean or the strings true/false. */
  private static boolean toBoolean(Object value) {
    if (value instanceof Boolean b) {
      return b;
    }
    String s = String.valueOf(value).trim();
    if (s.equalsIgnoreCase("true")) {
      return true;
    }
    if (s.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException("not a boolean: " + value);
  }
}
