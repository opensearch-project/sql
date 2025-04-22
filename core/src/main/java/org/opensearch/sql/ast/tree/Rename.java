/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
public class Rename extends UnresolvedPlan {
  private final List<Map> renameList;
  private UnresolvedPlan child;

  public Rename(List<Map> renameList, UnresolvedPlan child) {
    this.renameList = renameList;
    this.child = child;
    validate();
  }

  public Rename(List<Map> renameList) {
    this.renameList = renameList;
    validate();
  }

  private void validate() {
    renameList.forEach(rename -> validate(rename.getTarget()));
  }

  private void validate(UnresolvedExpression expr) {
    if (expr instanceof Field field) {
      String name = field.getField().toString();
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(name)) {
        throw new IllegalArgumentException(
            String.format("Cannot use metadata field [%s] in Rename command.", name));
      }
    }
  }

  @Override
  public Rename attach(UnresolvedPlan child) {
    if (null == this.child) {
      this.child = child;
    } else {
      this.child.attach(child);
    }
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRename(this, context);
  }
}
