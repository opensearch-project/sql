/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.*;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.common.utils.StringUtils;

/** AST node represent Transpose operation. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Transpose extends UnresolvedPlan {
  private final @NonNull java.util.Map<String, Argument> arguments;
  private UnresolvedPlan child;
  private static final int MAX_LIMIT_TRANSPOSE = 10000;
  private int maxRows = 5;
  private String columnName = "column";

  public Integer getMaxRows() {

    if (arguments.containsKey("number") && arguments.get("number").getValue() != null) {
      try {
        maxRows = Integer.parseInt(arguments.get("number").getValue().toString());
      } catch (NumberFormatException e) {
        // log warning and use default

      }
    }
    if (maxRows > MAX_LIMIT_TRANSPOSE) {
      throw new IllegalArgumentException(
          StringUtils.format("Maximum limit to transpose is %s", MAX_LIMIT_TRANSPOSE));
    }
    return maxRows;
  }

  public String getColumnName() {

    if (arguments.containsKey("columnName") && arguments.get("columnName").getValue() != null) {
      columnName = arguments.get("columnName").getValue().toString();
    }
    return columnName;
  }

  @Override
  public Transpose attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTranspose(this, context);
  }
}
