/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName;
import org.apache.calcite.runtime.variant.VariantSqlValue;
import org.apache.calcite.runtime.variant.VariantValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * One value of a column whose type varies from row to row, carrying the type it was written with,
 * so {@code 12.5} stays a number and {@code "4"} stays text. See {@link ExprVariantTupleValue}.
 *
 * <p>It delegates to Calcite's own variant implementation except in two places, which are why it
 * exists rather than the stock {@code VariantNonNull}:
 *
 * <ul>
 *   <li>a cast to text renders the value ({@code 12.5} as {@code "12.5"}), where Calcite answers
 *       null for a value not written as text. For an OpenSearch {@code flat_object} that text is
 *       the term the index holds, so a filter evaluated here agrees with the same filter pushed
 *       down.
 *   <li>an array is an {@link Array}, not one of Calcite's composite variants, which convert every
 *       element to one declared element type, and such a column may hold a number next to a string.
 * </ul>
 */
public class ExprVariantValue extends VariantSqlValue {

  private final VariantValue delegate;

  @Getter private final Object value;

  ExprVariantValue(RoundingMode roundingMode, Object value, RuntimeTypeInformation type) {
    super(type.getTypeName());
    this.value = value;
    this.delegate = VariantSqlValue.create(roundingMode, value, type);
  }

  @Override
  public @Nullable Object cast(RuntimeTypeInformation type) {
    if (type.getTypeName() == RuntimeSqlTypeName.VARCHAR) {
      // The index holds every leaf as the text of the value it was written with -- the term for
      // 12.5 is "12.5" and for true is "true" -- so a leaf read here must render as that same
      // text. Calcite's own cast answers null for a leaf not written as text, which would make a
      // filter evaluated here disagree with the one pushed down as a term query.
      return String.valueOf(value);
    }
    return delegate.cast(type);
  }

  @Override
  public @Nullable Object item(Object index) {
    return delegate.item(index);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return o instanceof ExprVariantValue other
        ? delegate.equals(other.delegate)
        : delegate.equals(o);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public static class Array extends VariantSqlValue {

    @Getter private final List<@Nullable VariantValue> elements;

    Array(List<@Nullable VariantValue> elements) {
      super(RuntimeSqlTypeName.ARRAY);
      this.elements = List.copyOf(elements);
    }

    @Override
    public @Nullable Object cast(RuntimeTypeInformation type) {
      return null;
    }

    @Override
    public @Nullable Object item(Object index) {
      return null;
    }

    List<@Nullable Object> unwrapAll() {
      List<@Nullable Object> result = new ArrayList<>(elements.size());
      for (VariantValue element : elements) {
        result.add(element == null ? null : ExprVariantTupleValue.unwrap(element));
      }
      return result;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return o instanceof Array other && elements.equals(other.elements);
    }

    @Override
    public int hashCode() {
      return elements.hashCode();
    }

    @Override
    public String toString() {
      return elements.toString();
    }
  }
}
