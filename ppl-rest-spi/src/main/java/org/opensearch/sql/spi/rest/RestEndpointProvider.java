/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;

/**
 * The extension point a plugin implements to contribute read-only {@code rest} endpoints. A plugin
 * registers a provider through {@code ExtensiblePlugin.loadExtensions(RestEndpointProvider.class)}
 * (a {@code META-INF/services} entry plus {@code extended.plugins=opensearch-sql}); the sql plugin
 * merges every discovered provider with its own built-in provider into one registry, so a built-in
 * endpoint and an externally contributed endpoint are uniform clients of the same contract.
 *
 * <p>A provider declares data (endpoint name, fixed schema, accepted args) and a handler; it never
 * touches the PPL grammar. The single {@code rest <name>} command routes to whichever provider
 * registered {@code <name>}.
 */
public interface RestEndpointProvider {

  /** The endpoints this provider contributes. Called once when the registry is built. */
  List<RestEndpointDefinition> getEndpoints();
}
