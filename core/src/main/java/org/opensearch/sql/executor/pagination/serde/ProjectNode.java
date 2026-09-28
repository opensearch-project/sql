/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * Serializable representation of a {@link org.opensearch.sql.planner.physical.ProjectOperator}.
 * Each expression in the project list is stored as its JSON-encoded (Base64) form produced by the
 * Phase-2 expression codec.
 */
public record ProjectNode(
    @JsonProperty("projectList") List<String> projectList,
    @JsonProperty("child") SerializablePlanNode child)
    implements SerializablePlanNode {

  @JsonCreator
  public ProjectNode(
      @JsonProperty("projectList") List<String> projectList,
      @JsonProperty("child") SerializablePlanNode child) {
    this.projectList = List.copyOf(Objects.requireNonNull(projectList, "projectList"));
    this.child = Objects.requireNonNull(child, "child");
  }
}
