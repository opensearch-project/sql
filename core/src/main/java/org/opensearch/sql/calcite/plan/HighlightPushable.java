/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.rel.RelNode;

/**
 * Interface for scan operators that support highlight push-down. Allows the visitor in the core
 * module to push highlight configuration to the scan without knowing the concrete scan type.
 */
public interface HighlightPushable {

  /**
   * Returns a new scan with the {@code _highlight} column added to its schema and the highlight
   * arguments stored for later use during execution.
   *
   * @param highlightArgs the highlight arguments — {@code ["*"]} to highlight search query matches
   *     in all fields, or specific terms like {@code ["login", "logout"]} to highlight
   * @return a new scan RelNode with highlight support
   */
  RelNode pushDownHighlight(List<String> highlightArgs);
}
