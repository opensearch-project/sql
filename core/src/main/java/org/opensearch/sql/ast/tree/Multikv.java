/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;

/**
 * AST node representing the {@code multikv} PPL command.
 *
 * <p>{@code multikv} extracts field values from an input field (default {@code _raw}) and emits one
 * row per source row. The input field is either table-formatted text (split into columns) or an
 * array of objects (one row per element, each declared column read from the element). This is a
 * one-to-many (row-multiplying) command.
 *
 * <p>The output column names must be determinable at plan time, sourced from the declared {@link
 * #fields} list, a literal {@link #forceHeader} line, or positional naming when {@link #noHeader}
 * is set. Runtime header auto-detection (no fields, no forceheader, no noheader) is not supported;
 * such a query is rejected at the field-resolution phase with a message directing the author to add
 * a {@code fields} clause.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
public class Multikv extends UnresolvedPlan {

  /** Default Splunk input field for multikv. */
  public static final String DEFAULT_INPUT_FIELD = "_raw";

  private UnresolvedPlan child;

  /** Input field carrying the table text. Defaults to {@code _raw}. */
  private final String inField;

  /** Declared output columns (the {@code fields} option). Null when not declared. */
  @Nullable private final List<Field> fields;

  /** Filter terms; a table row is kept only if it contains at least one term. Null when absent. */
  @Nullable private final List<String> filterTerms;

  /** 1-based header line to force (the {@code forceheader} option). Null when absent. */
  @Nullable private final Integer forceHeader;

  /** When true, columns are named positionally (Column_1, Column_2, ...). */
  private final boolean noHeader;

  /** When true (default), the original event is dropped from the output. */
  private final boolean rmOrig;

  public Multikv(
      String inField,
      @Nullable List<Field> fields,
      @Nullable List<String> filterTerms,
      @Nullable Integer forceHeader,
      boolean noHeader,
      boolean rmOrig) {
    this.inField = inField;
    this.fields = fields;
    this.filterTerms = filterTerms;
    this.forceHeader = forceHeader;
    this.noHeader = noHeader;
    this.rmOrig = rmOrig;
  }

  @Override
  public Multikv attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMultikv(this, context);
  }
}
