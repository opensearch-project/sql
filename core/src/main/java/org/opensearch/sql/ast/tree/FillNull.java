/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@RequiredArgsConstructor
@AllArgsConstructor
public class FillNull extends UnresolvedPlan {

  @Getter
  @RequiredArgsConstructor
  public static class NullableFieldFill {
    @NonNull private final Field nullableFieldReference;
    @NonNull private final UnresolvedExpression replaceNullWithMe;
  }

  public interface ContainNullableFieldFill {
    List<NullableFieldFill> getNullFieldFill();

    static ContainNullableFieldFill ofVariousValue(List<NullableFieldFill> replacements) {
      return new VariousValueNullFill(replacements);
    }

    static ContainNullableFieldFill ofSameValue(
        UnresolvedExpression replaceNullWithMe, List<Field> nullableFieldReferences) {
      return new SameValueNullFill(replaceNullWithMe, nullableFieldReferences);
    }
  }

  private static class SameValueNullFill implements ContainNullableFieldFill {
    @Getter(onMethod_ = @Override)
    private final List<NullableFieldFill> nullFieldFill;

    public SameValueNullFill(
        UnresolvedExpression replaceNullWithMe, List<Field> nullableFieldReferences) {
      Objects.requireNonNull(replaceNullWithMe, "Null replacement is required");
      this.nullFieldFill =
          Objects.requireNonNull(nullableFieldReferences, "Nullable field reference is required")
              .stream()
              .map(nullableReference -> new NullableFieldFill(nullableReference, replaceNullWithMe))
              .toList();
    }
  }

  @RequiredArgsConstructor
  private static class VariousValueNullFill implements ContainNullableFieldFill {
    @NonNull
    @Getter(onMethod_ = @Override)
    private final List<NullableFieldFill> nullFieldFill;
  }

  private UnresolvedPlan child;

  @NonNull private final ContainNullableFieldFill containNullableFieldFill;

  public List<NullableFieldFill> getNullableFieldFills() {
    return containNullableFieldFill.getNullFieldFill();
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return child == null ? List.of() : List.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFillNull(this, context);
  }
}
