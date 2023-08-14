/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.executor.adapter;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.core.action.ActionResponse;
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
