/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer.rule.read;

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
 * Rule that replace logical relation operator to {@link TableScanBuilder} for later
 * push down optimization. All push down optimization rules that depends on table scan
 * builder needs to run after this.
 */
public class CreateTableScanBuilder implements Rule<LogicalRelation> {

  /** Capture the table inside matched logical relation operator. */
  private final Capture<Table> capture;

  /** Pattern that matches logical relation operator. */
  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalRelation> pattern;

  /**
   * Construct create table scan builder rule.
   */
  public CreateTableScanBuilder() {
    this.capture = Capture.newCapture();
    this.pattern = Pattern.typeOf(LogicalRelation.class)
        .with(table().capturedAs(capture));
  }

  @Override
  public LogicalPlan apply(LogicalRelation plan, Captures captures) {
    TableScanBuilder scanBuilder = captures.get(capture).createScanBuilder();
    // TODO: Remove this after Prometheus refactored to new table scan builder too
    return (scanBuilder == null) ? plan : scanBuilder;
  }
}
