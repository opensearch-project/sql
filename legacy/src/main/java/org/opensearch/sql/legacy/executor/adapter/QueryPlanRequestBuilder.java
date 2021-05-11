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

package org.opensearch.sql.legacy.executor.adapter;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionResponse;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.BindingTupleQueryPlanner;
import org.opensearch.sql.legacy.query.planner.core.ColumnNode;

/**
 * The definition of QueryPlan SqlElasticRequestBuilder.
 */
@RequiredArgsConstructor
public class QueryPlanRequestBuilder implements SqlElasticRequestBuilder {
    private final BindingTupleQueryPlanner queryPlanner;

    public List<BindingTuple> execute() {
        return queryPlanner.execute();
    }

    public List<ColumnNode> outputColumns() {
        return queryPlanner.getColumnNodes();
    }

    @Override
    public String explain() {
        return queryPlanner.explain();
    }

    @Override
    public ActionRequest request() {
        throw new RuntimeException("unsupported operation");
    }

    @Override
    public ActionResponse get() {
        throw new RuntimeException("unsupported operation");
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        throw new RuntimeException("unsupported operation");
    }
}
