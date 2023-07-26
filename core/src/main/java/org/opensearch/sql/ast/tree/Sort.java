/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;

/** AST node for Sort {@link Sort#sortList} represent a list of sort expression and sort options. */
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@AllArgsConstructor
public class Sort extends UnresolvedPlan {
  private UnresolvedPlan child;
  private final List<Field> sortList;

  @Override
  public Sort attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitSort(this, context);
  }

  /** Sort Options. */
  @Data
  public static class SortOption {

    /** Default ascending sort option, null first. */
    public static SortOption DEFAULT_ASC = new SortOption(ASC, NULL_FIRST);

    /** Default descending sort option, null last. */
    public static SortOption DEFAULT_DESC = new SortOption(DESC, NULL_LAST);

    private final SortOrder sortOrder;
    private final NullOrder nullOrder;
  }

  public enum SortOrder {
    ASC,
    DESC
  }

  public enum NullOrder {
    NULL_FIRST,
    NULL_LAST
  }
}
