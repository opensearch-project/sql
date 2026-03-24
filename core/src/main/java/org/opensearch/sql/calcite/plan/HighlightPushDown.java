/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.ast.tree.HighlightConfig;

/**
 * Interface for scan nodes that support highlight pushdown. Highlight is a scan hint (not a
 * relational operator), so it is pushed down eagerly during plan construction rather than via an
 * optimizer rule.
 */
public interface HighlightPushDown {
  RelNode pushDownHighlight(HighlightConfig highlightConfig);
}
