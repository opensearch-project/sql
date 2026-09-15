/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model.datasource;

/**
 *
 * @opensearch.experimental
 *
 * Interface for results from various data sources.
 *
 * <p>Concrete result types are dispatched by the {@code dataSourceType} string carried alongside
 * the serialized payload in the transport protocol (see {@code ExecuteDirectQueryActionResponse}),
 * so no in-JSON Jackson type discriminator is used here.
 */
public interface DataSourceResult {}
