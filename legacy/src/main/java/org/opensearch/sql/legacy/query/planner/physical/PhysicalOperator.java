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

package org.opensearch.sql.legacy.query.planner.physical;

import java.util.Iterator;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;

/**
 * Physical operator
 */
public interface PhysicalOperator<T> extends PlanNode, Iterator<Row<T>>, AutoCloseable {

    /**
     * Estimate the cost of current physical operator
     *
     * @return cost
     */
    Cost estimate();


    /**
     * Initialize operator.
     *
     * @param params exuecution parameters needed
     */
    default void open(ExecuteParams params) throws Exception {
        for (PlanNode node : children()) {
            ((PhysicalOperator) node).open(params);
        }
    }


    /**
     * Close resources related to the operator.
     *
     * @throws Exception potential exception raised
     */
    @Override
    default void close() {
        for (PlanNode node : children()) {
            ((PhysicalOperator) node).close();
        }
    }
}
