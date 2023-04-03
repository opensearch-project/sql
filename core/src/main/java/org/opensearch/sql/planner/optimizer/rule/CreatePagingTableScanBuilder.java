/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static org.opensearch.sql.planner.optimizer.pattern.Patterns.table;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Rule to create a paged TableScanBuilder in pagination request.
 */
public class CreatePagingTableScanBuilder implements Rule<LogicalRelation> {
  /** Capture the table inside matched logical relation operator. */
  private final Capture<Table> capture;

  /** Pattern that matches logical relation operator. */
  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalRelation> pattern;

  /**
   * Constructor.
   */
  public CreatePagingTableScanBuilder() {
    this.capture = Capture.newCapture();
    this.pattern = Pattern.typeOf(LogicalRelation.class)
        .with(table().capturedAs(capture));
  }

  @Override
  public LogicalPlan apply(LogicalRelation plan, Captures captures) {
    TableScanBuilder scanBuilder = captures.get(capture).createPagedScanBuilder();
    // TODO: Remove this after Prometheus refactored to new table scan builder too
    return (scanBuilder == null) ? plan : scanBuilder;
  }
}
