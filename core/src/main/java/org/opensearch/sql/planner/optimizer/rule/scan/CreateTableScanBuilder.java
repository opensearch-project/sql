/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule.scan;

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
import org.opensearch.sql.storage.TableScanBuilder;

/**
 * {@code RelationPushDown} defines a set of push down rules...
 */
public class CreateTableScanBuilder implements Rule<LogicalRelation> {

  private final Capture<Table> capture;

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
    // TODO: temporary check to avoid impact and separate Prometheus refactor work
    return (scanBuilder == null) ? plan : scanBuilder;
  }
}
