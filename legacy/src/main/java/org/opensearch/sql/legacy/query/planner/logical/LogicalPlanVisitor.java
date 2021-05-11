/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.query.planner.logical;

import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;
import org.opensearch.sql.legacy.query.planner.logical.node.Filter;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;
import org.opensearch.sql.legacy.query.planner.logical.node.Join;
import org.opensearch.sql.legacy.query.planner.logical.node.Project;
import org.opensearch.sql.legacy.query.planner.logical.node.Sort;
import org.opensearch.sql.legacy.query.planner.logical.node.TableScan;
import org.opensearch.sql.legacy.query.planner.logical.node.Top;

/**
 * Transformation rule for logical plan tree optimization implemented by standard Visitor pattern.
 */
public interface LogicalPlanVisitor extends Visitor {

    @Override
    default boolean visit(PlanNode op) {
        if (op instanceof Project) {
            return visit((Project) op);
        } else if (op instanceof Filter) {
            return visit((Filter) op);
        } else if (op instanceof Join) {
            return visit((Join) op);
        } else if (op instanceof Group) {
            return visit((Group) op);
        } else if (op instanceof TableScan) {
            return visit((TableScan) op);
        } else if (op instanceof Top) {
            return visit((Top) op);
        } else if (op instanceof Sort) {
            return visit((Sort) op);
        }
        throw new IllegalArgumentException("Unknown operator type: " + op);
    }

    @Override
    default void endVisit(PlanNode op) {
        if (op instanceof Project) {
            endVisit((Project) op);
        } else if (op instanceof Filter) {
            endVisit((Filter) op);
        } else if (op instanceof Join) {
            endVisit((Join) op);
        } else if (op instanceof Group) {
            endVisit((Group) op);
        } else if (op instanceof TableScan) {
            endVisit((TableScan) op);
        } else if (op instanceof Top) {
            endVisit((Top) op);
        } else if (op instanceof Sort) {
            endVisit((Sort) op);
        } else {
            throw new IllegalArgumentException("Unknown operator type: " + op);
        }
    }

    default boolean visit(Project project) {
        return true;
    }

    default void endVisit(Project project) {
    }

    default boolean visit(Filter filter) {
        return true;
    }

    default void endVisit(Filter filter) {
    }

    default boolean visit(Join join) {
        return true;
    }

    default void endVisit(Join join) {
    }

    default boolean visit(Group group) {
        return true;
    }

    default void endVisit(Group group) {
    }

    default boolean visit(TableScan scan) {
        return true;
    }

    default void endVisit(TableScan scan) {
    }

    default boolean visit(Top top) {
        return true;
    }

    default void endVisit(Top top) {
    }

    default boolean visit(Sort sort) {
        return true;
    }

    default void endVisit(Sort sort) {
    }
}
