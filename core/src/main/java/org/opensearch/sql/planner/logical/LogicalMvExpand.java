/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;
//
// import java.util.Collections;
// import java.util.List;
// import java.util.Optional;
// import lombok.EqualsAndHashCode;
// import org.opensearch.sql.ast.expression.Field;
//
// @EqualsAndHashCode(callSuper = true)
// public class LogicalMvExpand extends LogicalPlan {
//    private final Field field;
//    private final Optional<Integer> limit;
//
//    public LogicalMvExpand(LogicalPlan input, Field field, Optional<Integer> limit) {
//        super(Collections.singletonList(input));
//        this.field = field;
//        this.limit = limit != null ? limit : Optional.empty();
//    }
//
//    public LogicalPlan getInput() {
//        return getChild().get(0);
//    }
//
//    public Field getField() {
//        return field;
//    }
//
//    public Optional<Integer> getLimit() {
//        return limit;
//    }
//
//    @Override
//    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
//        return visitor.visitLogicalMvExpand(this, context);
//    }
//
//    @Override
//    public String toString() {
//        return String.format("LogicalMvExpand(field=%s, limit=%s)", field, limit);
//    }
// }
