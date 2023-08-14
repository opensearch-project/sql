/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.query;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.core.action.ActionResponse;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlOpenSearchRequestBuilder implements SqlElasticRequestBuilder {
    ActionRequestBuilder requestBuilder;

    public SqlOpenSearchRequestBuilder(ActionRequestBuilder requestBuilder) {
        this.requestBuilder = requestBuilder;
    }

    @Override
    public ActionRequest request() {
        return requestBuilder.request();
    }

    @Override
    public String explain() {
        return requestBuilder.toString();
    }

    @Override
    public ActionResponse get() {
        return requestBuilder.get();
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return requestBuilder;
    }

    @Override
    public String toString() {
        return this.requestBuilder.toString();
    }
}
