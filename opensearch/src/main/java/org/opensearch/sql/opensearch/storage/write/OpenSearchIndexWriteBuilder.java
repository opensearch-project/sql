/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.function.Function;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.write.TableWriteBuilder;
import org.opensearch.sql.storage.write.TableWriteOperator;

/**
 * Builder implementation for OpenSearch index write operator.
 */
public class OpenSearchIndexWriteBuilder extends TableWriteBuilder {

  /** OpenSearch index write operator build function. */
  private final Function<PhysicalPlan, OpenSearchIndexWrite> buildIndexWrite;

  /**
   * Construct OpenSearch index write builder with child node and build function.
   */
  public OpenSearchIndexWriteBuilder(LogicalPlan child,
                                     Function<PhysicalPlan, OpenSearchIndexWrite> buildIndexWrite) {
    super(child);
    this.buildIndexWrite = buildIndexWrite;
  }

  @Override
  public TableWriteOperator build(PhysicalPlan child) {
    return buildIndexWrite.apply(child);
  }
}
