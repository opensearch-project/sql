/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import lombok.Getter;
import lombok.Setter;

/**
 * The context used for JsonSupportVisitor.
 */
public class JsonSupportVisitorContext {
  @Getter
  @Setter
  private boolean isVisitingProject = false;
}
