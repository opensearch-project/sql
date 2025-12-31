/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * AST node representing the Convert command.
 *
 * <p>Syntax: convert [timeformat="format"] function(fields) [AS alias], ...
 *
 * <p>Example: convert auto(age), num(price) AS numeric_price
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Convert extends UnresolvedPlan {
  /** Reserved for future time conversion functions (ctime, mktime, mstime). */
  private final String timeformat;

  /** List of conversion functions to apply. */
  private final List<ConvertFunction> convertFunctions;

  /** Child plan node. */
  private UnresolvedPlan child;

  @Override
  public Convert attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitConvert(this, context);
  }
}
