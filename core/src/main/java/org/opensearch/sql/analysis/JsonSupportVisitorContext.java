/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * The context used for JsonSupportVisitor.
 */
public class JsonSupportVisitorContext {
  @Getter
  @Setter
  private boolean isVisitingProject = false;

  @Getter
  @Setter
  private List<String> unsupportedNodes = new ArrayList<>();

  public void addToUnsupportedNodes(String unsupportedNode) {
    unsupportedNodes.add(unsupportedNode);
  }
}
