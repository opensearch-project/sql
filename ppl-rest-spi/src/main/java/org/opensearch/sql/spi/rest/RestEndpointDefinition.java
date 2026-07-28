/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;
import java.util.Objects;

/**
 * One read-only {@code rest} endpoint from a {@link RestEndpointProvider}: a unique name (the token
 * after {@code rest}, e.g. {@code /_cluster/health}), a fixed {@link Column} schema, the {@link
 * ArgSpec} it accepts, and the {@link RestEndpointHandler} that produces its rows. Immutable; build
 * with {@link #builder()}.
 */
public interface RestEndpointDefinition {

  String name();

  List<Column> schema();

  ArgSpec argSpec();

  RestEndpointHandler handler();

  static Builder builder() {
    return new Builder();
  }

  final class Builder {
    private String name;
    private List<Column> schema = List.of();
    private ArgSpec argSpec = ArgSpec.NONE;
    private RestEndpointHandler handler;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder schema(List<Column> schema) {
      this.schema = schema;
      return this;
    }

    public Builder argSpec(ArgSpec argSpec) {
      this.argSpec = argSpec;
      return this;
    }

    public Builder handler(RestEndpointHandler handler) {
      this.handler = handler;
      return this;
    }

    public RestEndpointDefinition build() {
      String endpointName = Objects.requireNonNull(name, "rest endpoint name is required");
      List<Column> cols = List.copyOf(Objects.requireNonNull(schema, "schema is required"));
      ArgSpec spec = Objects.requireNonNull(argSpec, "argSpec is required");
      RestEndpointHandler endpointHandler = Objects.requireNonNull(handler, "handler is required");
      return new RestEndpointDefinition() {
        @Override
        public String name() {
          return endpointName;
        }

        @Override
        public List<Column> schema() {
          return cols;
        }

        @Override
        public ArgSpec argSpec() {
          return spec;
        }

        @Override
        public RestEndpointHandler handler() {
          return endpointHandler;
        }
      };
    }
  }
}
