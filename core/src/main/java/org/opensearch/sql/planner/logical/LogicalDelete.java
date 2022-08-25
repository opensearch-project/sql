/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.opensearch.sql.storage.Table;

@Getter
public class LogicalDelete extends LogicalPlan {

  private final String tableName;

  private final Table table;

  public LogicalDelete(LogicalPlan child, String tableName, Table table) {
    super(ImmutableList.of());
    this.tableName = tableName;
    this.table = table;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDelete(this, context);
  }
}
