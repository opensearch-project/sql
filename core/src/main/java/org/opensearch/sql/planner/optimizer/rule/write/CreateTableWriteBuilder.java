/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule.write;

import static org.opensearch.sql.planner.optimizer.pattern.Patterns.writeTable;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.write.TableWriteBuilder;

/**
 * Rule that replaces logical write operator with {@link TableWriteBuilder} for later optimization
 * and transforming to physical operator.
 */
public class CreateTableWriteBuilder implements Rule<LogicalWrite> {

  /** Capture the table inside matched logical relation operator. */
  private final Capture<Table> capture;

  /** Pattern that matches logical relation operator. */
  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalWrite> pattern;

  /** Construct create table write builder rule. */
  public CreateTableWriteBuilder() {
    this.capture = Capture.newCapture();
    this.pattern = Pattern.typeOf(LogicalWrite.class).with(writeTable().capturedAs(capture));
  }

  @Override
  public LogicalPlan apply(LogicalWrite plan, Captures captures) {
    return captures.get(capture).createWriteBuilder(plan);
  }
}
