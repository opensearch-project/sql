/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexScan;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

@Getter
public class MergeLimitAndIndexScan implements Rule<LogicalLimit> {

  private final Capture<OpenSearchLogicalIndexScan> indexScanCapture;

  @Accessors(fluent = true)
  private final Pattern<LogicalLimit> pattern;

  /**
   * Constructor of MergeLimitAndIndexScan.
   */
  public MergeLimitAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalLimit.class)
        .with(source()
            .matching(typeOf(OpenSearchLogicalIndexScan.class).capturedAs(indexScanCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalLimit plan, Captures captures) {
    OpenSearchLogicalIndexScan indexScan = captures.get(indexScanCapture);
    OpenSearchLogicalIndexScan.OpenSearchLogicalIndexScanBuilder builder =
        OpenSearchLogicalIndexScan.builder();
    builder.relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .offset(plan.getOffset())
        .limit(plan.getLimit())
        .highlightField(indexScan.getHighlightField());
    if (indexScan.getSortList() != null) {
      builder.sortList(indexScan.getSortList());
    }
    return builder.build();
  }
}
