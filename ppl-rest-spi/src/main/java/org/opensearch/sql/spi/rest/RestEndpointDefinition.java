/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.Objects;

/**
 * One read-only {@code rest} endpoint from a {@link RestEndpointProvider}: a unique name (the token
 * after {@code rest}, e.g. {@code /_cluster/health}), the {@link ArgSpec} it accepts, and the
 * {@link RestEndpointHandler} that produces its rows. Every endpoint surfaces a single {@code
 * response} string column; a query extracts the fields it needs with {@code spath} or {@code
 * json_extract}. A provider that needs to mask sensitive values does so inside its handler before
 * returning the response. Immutable; build with {@link #builder()}.
 */
public interface RestEndpointDefinition {

  String name();

  ArgSpec argSpec();

  RestEndpointHandler handler();

  static Builder builder() {
    return new Builder();
  }

  final class Builder {
    private String name;
    private ArgSpec argSpec = ArgSpec.NONE;
    private RestEndpointHandler handler;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
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
      ArgSpec spec = Objects.requireNonNull(argSpec, "argSpec is required");
      RestEndpointHandler endpointHandler = Objects.requireNonNull(handler, "handler is required");
      return new RestEndpointDefinition() {
        @Override
        public String name() {
          return endpointName;
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
