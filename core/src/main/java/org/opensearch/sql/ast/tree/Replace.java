/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Replace extends UnresolvedPlan {
  private final UnresolvedExpression pattern;
  private final UnresolvedExpression replacement;
  private final List<Field> fieldList;
  private UnresolvedPlan child;

  public Replace(
      UnresolvedExpression pattern, UnresolvedExpression replacement, List<Field> fieldList) {
    this.pattern = pattern;
    this.replacement = replacement;
    this.fieldList = fieldList;
    validate();
  }

  public void validate() {
    if (pattern == null) {
      throw new IllegalArgumentException("Pattern expression cannot be null in Replace command");
    }
    if (replacement == null) {
      throw new IllegalArgumentException(
          "Replacement expression cannot be null in Replace command");
    }

    // Validate pattern is a string literal
    if (!(pattern instanceof Literal && ((Literal) pattern).getType() == DataType.STRING)) {
      throw new IllegalArgumentException("Pattern must be a string literal in Replace command");
    }

    // Validate replacement is a string literal
    if (!(replacement instanceof Literal && ((Literal) replacement).getType() == DataType.STRING)) {
      throw new IllegalArgumentException("Replacement must be a string literal in Replace command");
    }

    if (fieldList == null || fieldList.isEmpty()) {
      throw new IllegalArgumentException(
          "Field list cannot be empty in Replace command. Use IN clause to specify the field.");
    }

    Set<String> uniqueFields = new HashSet<>();
    List<String> duplicates =
        fieldList.stream()
            .map(field -> field.getField().toString())
            .filter(fieldName -> !uniqueFields.add(fieldName))
            .collect(Collectors.toList());

    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Duplicate fields [%s] in Replace command", String.join(", ", duplicates)));
    }
  }

  @Override
  public Replace attach(UnresolvedPlan child) {
    if (null == this.child) {
      this.child = child;
    } else {
      this.child.attach(child);
    }
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitReplace(this, context);
  }
}
