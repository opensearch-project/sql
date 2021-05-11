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

package org.opensearch.sql.legacy.query.planner.physical.estimation;

import static java.util.Comparator.comparing;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.logical.LogicalPlanVisitor;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/**
 * Convert and estimate the cost of each operator and generate one optimal plan.
 * Memorize cost of candidate physical operators in the bottom-up way to avoid duplicate computation.
 */
public class Estimation<T> implements LogicalPlanVisitor {

    /**
     * Optimal physical operator for logical operator based on completed estimation
     */
    private Map<LogicalOperator, PhysicalOperator<T>> optimalOps = new IdentityHashMap<>();

    /**
     * Keep tracking of the operator that exit visit()
     */
    private PhysicalOperator<T> root;

    @Override
    public boolean visit(Group group) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void endVisit(PlanNode node) {
        LogicalOperator op = (LogicalOperator) node;
        PhysicalOperator<T> optimal = Arrays.stream(op.toPhysical(optimalOps)).
                min(comparing(PhysicalOperator::estimate)).
                orElseThrow(() -> new IllegalStateException(
                        "No optimal operator found: " + op));
        optimalOps.put(op, optimal);
        root = optimal;
    }

    public PhysicalOperator<T> optimalPlan() {
        return root;
    }
}
