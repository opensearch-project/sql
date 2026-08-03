/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.Redactor;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointHandler;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * The read-only endpoint allow-list, built by merging every {@link RestEndpointProvider} (the
 * built-in {@link CoreEndpointsProvider} plus any externally contributed providers) into one map of
 * endpoint name to an internal {@link Endpoint}. A built-in and an externally contributed endpoint
 * are uniform entries here, except that a built-in name cannot be shadowed by an external provider.
 *
 * <p>This is the single place the read-only allow-list is enforced. An endpoint that no provider
 * registered, including every mutating endpoint, is rejected by {@link #resolve} with a clear
 * exception, and an arg the endpoint's {@link ArgSpec} does not accept is rejected by {@link
 * #validate}. Adding an endpoint is a reviewed change to a provider, never arbitrary pass-through.
 */
public final class RestEndpointRegistry {

  private static final Logger LOG = LogManager.getLogger(RestEndpointRegistry.class);

  private final Map<String, Endpoint> registry;

  public RestEndpointRegistry(List<RestEndpointProvider> providers) {
    Map<String, Endpoint> m = new LinkedHashMap<>();
    Set<String> disabled = new HashSet<>();
    for (RestEndpointProvider provider : providers) {
      boolean builtIn = provider instanceof CoreEndpointsProvider;
      for (RestEndpointDefinition definition : provider.getEndpoints()) {
        String name = definition.name();
        if (disabled.contains(name)) {
          continue;
        }
        Endpoint existing = m.get(name);
        if (existing == null) {
          m.put(name, new Endpoint(definition, builtIn));
          continue;
        }
        if (existing.isBuiltIn() || builtIn) {
          LOG.warn(
              "rest endpoint [{}] collides with a built-in endpoint; ignoring the duplicate from"
                  + " provider [{}]",
              name,
              provider.getClass().getName());
          continue;
        }
        LOG.warn(
            "rest endpoint [{}] is registered by multiple external providers; disabling it."
                + " Conflicting provider [{}]",
            name,
            provider.getClass().getName());
        m.remove(name);
        disabled.add(name);
      }
    }
    this.registry = m;
  }

  /** A single allow-listed endpoint, adapted from a provider's {@link RestEndpointDefinition}. */
  @Getter
  public static final class Endpoint {
    private final String path;
    private final LinkedHashMap<String, ExprType> schema;
    private final ArgSpec argSpec;
    private final RestEndpointHandler handler;
    private final boolean builtIn;

    Endpoint(RestEndpointDefinition definition, boolean builtIn) {
      this.path = definition.name();
      this.schema = toExprSchema(definition.schema());
      this.argSpec = definition.argSpec();
      this.handler = definition.handler();
      this.builtIn = builtIn;
    }

    /**
     * Invoke the provider's handler, apply the platform {@link Redactor} to each raw row (with the
     * endpoint name as scope), then shape it into fixed-schema rows. Runs at execution time (scan
     * open). {@link Redactor#NONE} (the OSS default) passes rows through unchanged.
     */
    public List<ExprValue> toRows(RestEndpointContext ctx, Redactor redactor) {
      List<ExprValue> out = new ArrayList<>();
      for (Map<String, Object> raw : handler.fetch(ctx)) {
        Map<String, Object> redacted = redactor.redact(path, raw);
        LinkedHashMap<String, ExprValue> tuple = new LinkedHashMap<>();
        for (Map.Entry<String, ExprType> col : schema.entrySet()) {
          tuple.put(col.getKey(), coerce(redacted.get(col.getKey())));
        }
        out.add(new ExprTupleValue(tuple));
      }
      return out;
    }
  }

  /**
   * Resolve an allow-listed endpoint. Anything no provider registered (unknown path, mutating verb,
   * {@code /services/*}, plugin admin endpoints) is refused here.
   */
  public Endpoint resolve(String path) {
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException(
          "rest endpoint must be a non-empty path. Only the following endpoints are supported: "
              + registry.keySet());
    }
    Endpoint endpoint = registry.get(path);
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "rest endpoint ["
              + path
              + "] is not allow-listed. Only the following endpoints are supported: "
              + registry.keySet());
    }
    return endpoint;
  }

  /** Validate the count, the reserved timeout token, and every supplied query arg. */
  public void validate(RestSpec spec) {
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
          "rest endpoint [" + spec.getEndpoint() + "] does not support the timeout argument yet");
    }
    if (spec.getArgs() != null) {
      ArgSpec argSpec = endpoint.getArgSpec();
      for (String arg : spec.getArgs().keySet()) {
        if (!argSpec.allows(arg)) {
          throw new IllegalArgumentException(
              "rest endpoint ["
                  + spec.getEndpoint()
                  + "] does not accept arg ["
                  + arg
                  + "]. Allowed args: "
                  + argSpec.allowedArgs());
        }
        argSpec.validateValue(spec.getEndpoint(), arg, spec.getArgs().get(arg));
      }
    }
  }

  private static LinkedHashMap<String, ExprType> toExprSchema(List<Column> columns) {
    LinkedHashMap<String, ExprType> schema = new LinkedHashMap<>();
    for (Column column : columns) {
      schema.put(column.name(), STRING);
    }
    return schema;
  }

  private static ExprValue coerce(Object value) {
    return value == null ? ExprNullValue.of() : stringValue(String.valueOf(value));
  }
}
