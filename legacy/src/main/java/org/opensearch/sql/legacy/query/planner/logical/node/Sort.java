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

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.sort.QuickSort;

/**
 * Logical operator for Sort.
 */
public class Sort implements LogicalOperator {

    private final LogicalOperator next;

    /**
     * Column name list in ORDER BY
     */
    private final List<String> orderByColNames;

    /**
     * Order by type, ex. ASC, DESC
     */
    private final String orderByType;


    public Sort(LogicalOperator next, List<String> orderByColNames, String orderByType) {
        this.next = next;
        this.orderByColNames = orderByColNames;
        this.orderByType = orderByType.toUpperCase();
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[]{next};
    }

    @Override
    public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
        return new PhysicalOperator[]{
                new QuickSort<>(optimalOps.get(next), orderByColNames, orderByType)
        };
    }

    @Override
    public String toString() {
        return "Sort [ columns=" + orderByColNames + " order=" + orderByType + " ]";
    }

}
