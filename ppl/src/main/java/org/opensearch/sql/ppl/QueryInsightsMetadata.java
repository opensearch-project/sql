/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.util.List;

/**
 * Best-effort metadata about a PPL query, derived once from the parsed AST that is executed, for
 * consumers such as Query Insights.
 *
 * @param anonymizedQuery the query with literals masked by {@code PPLQueryDataAnonymizer}
 * @param indices the source index name(s) the query reads from; empty when none are resolvable
 * @param explain whether the statement is an {@code explain}, which plans but runs no search. The
 *     endpoint alone cannot tell: {@code explain} is also valid inside the query text.
 */
public record QueryInsightsMetadata(
    String anonymizedQuery, List<String> indices, boolean explain) {}
