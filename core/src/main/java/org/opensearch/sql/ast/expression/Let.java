/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

/** Represent the assign operation. e.g. velocity = distance/speed. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Let extends UnresolvedExpression {
  private final Field var;
  private final UnresolvedExpression expression;

  public Let(Field var, UnresolvedExpression expression) {
    String varName = var.getField().toString();
    if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(varName)) {
      throw new IllegalArgumentException(
          String.format("Cannot use metadata field [%s] as the eval field.", varName));
    }
    this.var = var;
    this.expression = expression;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitLet(this, context);
  }
}
