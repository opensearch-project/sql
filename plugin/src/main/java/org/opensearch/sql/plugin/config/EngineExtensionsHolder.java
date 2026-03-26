/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.config;

import java.util.List;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Holds execution engine engines loaded via SPI. Returned from {@code SQLPlugin.createComponents()}
 * so that OpenSearch's Guice injector can inject it into transport actions like {@code
 * TransportPPLQueryAction}.
 */
public record EngineExtensionsHolder(List<ExecutionEngine> engines) {}
