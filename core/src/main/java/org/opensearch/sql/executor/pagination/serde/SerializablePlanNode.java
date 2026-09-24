/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination.serde;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface representing a serializable plan node for cursor pagination. Uses Jackson
 * polymorphic type info with a closed set of subtypes — unknown types are rejected at
 * deserialization time before any constructor runs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@t")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ProjectNode.class, name = "project"),
  @JsonSubTypes.Type(value = IndexScanNode.class, name = "indexScan")
})
public sealed interface SerializablePlanNode permits ProjectNode, IndexScanNode {}
