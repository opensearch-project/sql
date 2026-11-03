/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

/**
 * Metadata about a PPL query, derived once from the parsed AST that is executed, for consumers such
 * as tracing and Query Insights.
 *
 * @param anonymizedQuery the query with literals masked by {@code PPLQueryDataAnonymizer}
 * @param explain whether the statement is an {@code explain}, which plans but runs no search. The
 *     endpoint alone cannot tell: {@code explain} is also valid inside the query text.
 */
public record QueryInsightsMetadata(String anonymizedQuery, boolean explain) {}
