/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.query.planner.physical.node.scroll;

import java.util.Iterator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.AggregationQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.Row;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;

/**
 * The definition of Scroll Operator.
 */
@RequiredArgsConstructor
public class PhysicalScroll implements PhysicalOperator<BindingTuple> {
    private final QueryAction queryAction;

    private Iterator<BindingTupleRow> rowIterator;

    @Override
    public Cost estimate() {
        return null;
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[0];
    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public Row<BindingTuple> next() {
        return rowIterator.next();
    }

    @Override
    public void open(ExecuteParams params) {
        try {
            ActionResponse response = queryAction.explain().get();
            if (queryAction instanceof AggregationQueryAction) {
                rowIterator = SearchAggregationResponseHelper
                        .populateSearchAggregationResponse(((SearchResponse) response).getAggregations())
                        .iterator();
            } else {
                throw new IllegalStateException("Not support QueryAction type: " + queryAction.getClass());
            }
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    @Override
    public String toString() {
        return queryAction.explain().toString();
    }
}
