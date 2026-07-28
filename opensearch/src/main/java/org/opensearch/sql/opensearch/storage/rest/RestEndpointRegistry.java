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
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.ColumnType;
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
 * are uniform entries here; the built-in provider holds no privileged position.
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
    for (RestEndpointProvider provider : providers) {
      for (RestEndpointDefinition definition : provider.getEndpoints()) {
        if (m.containsKey(definition.name())) {
          LOG.warn(
              "rest endpoint [{}] is already registered; ignoring the duplicate from provider [{}]",
              definition.name(),
              provider.getClass().getName());
          continue;
        }
        m.put(definition.name(), new Endpoint(definition));
      }
    }
    this.registry = m;
  }

  public java.util.Set<String> endpointNames() {
    return registry.keySet();
  }

  /** A single allow-listed endpoint, adapted from a provider's {@link RestEndpointDefinition}. */
  @Getter
  public static final class Endpoint {
    private final String path;
    private final LinkedHashMap<String, ExprType> schema;
    private final ArgSpec argSpec;
    private final RestEndpointHandler handler;

    Endpoint(RestEndpointDefinition definition) {
      this.path = definition.name();
      this.schema = toExprSchema(definition.schema());
      this.argSpec = definition.argSpec();
      this.handler = definition.handler();
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
          tuple.put(col.getKey(), coerce(col.getKey(), col.getValue(), redacted.get(col.getKey())));
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
      schema.put(column.name(), toExprType(column.type()));
    }
    return schema;
  }

  private static ExprType toExprType(ColumnType type) {
    return switch (type) {
      case STRING -> STRING;
      case INTEGER -> INTEGER;
      case LONG -> LONG;
      case DOUBLE -> DOUBLE;
      case BOOLEAN -> BOOLEAN;
    };
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
    } catch (IllegalArgumentException | ClassCastException e) {
      // Surface a clear client error (HTTP 400) instead of a raw HTTP 500 when an endpoint
      // returns an unexpected value shape. NumberFormatException extends IllegalArgumentException,
      // so toNumber parse failures and toBoolean's "not a boolean" are both caught here; genuinely
      // unexpected faults (NPE, etc.) are left to propagate.
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

  private static Number toNumber(Object value) {
    if (value instanceof Number n) {
      return n;
    }
    String s = String.valueOf(value).trim();
    if (s.isEmpty()) {
      throw new NumberFormatException("empty string");
    }
    if (s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0) {
      return Double.parseDouble(s);
    }
    return Long.parseLong(s);
  }

  private static boolean toBoolean(Object value) {
    if (value instanceof Boolean b) {
      return b;
    }
    String s = String.valueOf(value).trim();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("empty string is not a boolean");
    }
    if (s.equalsIgnoreCase("true")) {
      return true;
    }
    if (s.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException("not a boolean: " + value);
  }
}
