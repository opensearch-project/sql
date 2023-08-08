/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.pattern;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.Property;
import com.facebook.presto.matching.PropertyPattern;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/** Pattern helper class. */
@UtilityClass
public class Patterns {

  /** Logical filter with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalFilter> filter(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalFilter.class).with(source(pattern));
  }

  /** Logical aggregate operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalAggregation> aggregate(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalAggregation.class).with(source(pattern));
  }

  /** Logical sort operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalSort> sort(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalSort.class).with(source(pattern));
  }

  /** Logical limit operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalLimit> limit(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalLimit.class).with(source(pattern));
  }

  /** Logical highlight operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalHighlight> highlight(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalHighlight.class).with(source(pattern));
  }

  /** Logical nested operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalNested> nested(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalNested.class).with(source(pattern));
  }

  /** Logical project operator with a given pattern on inner field. */
  public static <T extends LogicalPlan> Pattern<LogicalProject> project(Pattern<T> pattern) {
    return Pattern.typeOf(LogicalProject.class).with(source(pattern));
  }

  /** Pattern for {@link TableScanBuilder} and capture it meanwhile. */
  public static Pattern<TableScanBuilder> scanBuilder() {
    return Pattern.typeOf(TableScanBuilder.class).capturedAs(Capture.newCapture());
  }

  /** LogicalPlan source {@link Property}. */
  public static Property<LogicalPlan, LogicalPlan> source() {
    return Property.optionalProperty(
        "source",
        plan ->
            plan.getChild().size() == 1 ? Optional.of(plan.getChild().get(0)) : Optional.empty());
  }

  /** Source (children field) with a given pattern. */
  @SuppressWarnings("unchecked")
  public static <T extends LogicalPlan> PropertyPattern<LogicalPlan, T> source(Pattern<T> pattern) {
    Property<LogicalPlan, T> property =
        Property.optionalProperty(
            "source",
            plan ->
                plan.getChild().size() == 1
                    ? Optional.of((T) plan.getChild().get(0))
                    : Optional.empty());

    return property.matching(pattern);
  }

  /** Logical relation with table field. */
  public static Property<LogicalPlan, Table> table() {
    return Property.optionalProperty(
        "table",
        plan ->
            plan instanceof LogicalRelation
                ? Optional.of(((LogicalRelation) plan).getTable())
                : Optional.empty());
  }

  /** Logical write with table field. */
  public static Property<LogicalPlan, Table> writeTable() {
    return Property.optionalProperty(
        "table",
        plan ->
            plan instanceof LogicalWrite
                ? Optional.of(((LogicalWrite) plan).getTable())
                : Optional.empty());
  }
}
