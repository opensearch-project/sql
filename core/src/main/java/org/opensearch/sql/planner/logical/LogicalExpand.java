/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.logical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

import java.util.Collections;

@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalExpand extends LogicalPlan{

    @Getter
    private final Expression field;
    @Getter
    private final Expression alias;

    public LogicalExpand(LogicalPlan child, Expression field, Expression alias) {
        super(Collections.singletonList(child));
        this.field = field;
        this.alias = alias;
    }

    @Override
    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitExpand(this, context);
    }
}
