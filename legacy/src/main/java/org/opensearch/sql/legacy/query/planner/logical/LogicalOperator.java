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

import java.util.Map;
import org.json.JSONPropertyIgnore;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/**
 * Logical operator in logical plan tree.
 */
public interface LogicalOperator extends PlanNode {

    /**
     * If current operator is no operation. It depends on specific internal state of operator
     * <p>
     * Ignore this field in explanation because all explainable operator are NOT no-op.
     *
     * @return true if NoOp
     */
    @JSONPropertyIgnore
    default boolean isNoOp() {
        return false;
    }

    /**
     * Map logical operator to physical operators (possibly 1 to N mapping)
     * <p>
     * Note that generic type on PhysicalOperator[] would enforce all impl convert array to generic type array
     * because generic type array is unable to be created directly.
     *
     * @param optimalOps optimal physical operators estimated so far
     * @return list of physical operator
     */
    <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps);

}
