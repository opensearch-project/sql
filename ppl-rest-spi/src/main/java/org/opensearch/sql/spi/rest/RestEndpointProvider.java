/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;

/**
 * The extension point a plugin implements to contribute read-only {@code rest} endpoints, via
 * {@code ExtensiblePlugin.loadExtensions(RestEndpointProvider.class)}. The sql plugin merges every
 * discovered provider with its own built-in one into a single registry, so built-in and external
 * endpoints are uniform clients of the same contract. A provider declares data (name, schema, args)
 * and a handler; it never touches the PPL grammar.
 */
public interface RestEndpointProvider {

  /** The endpoints this provider contributes. Called once when the registry is built. */
  List<RestEndpointDefinition> getEndpoints();
}
