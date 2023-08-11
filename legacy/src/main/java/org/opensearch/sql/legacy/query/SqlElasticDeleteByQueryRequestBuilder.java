/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.query;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlElasticDeleteByQueryRequestBuilder implements SqlElasticRequestBuilder {
    DeleteByQueryRequestBuilder deleteByQueryRequestBuilder;

    public SqlElasticDeleteByQueryRequestBuilder(DeleteByQueryRequestBuilder deleteByQueryRequestBuilder) {
        this.deleteByQueryRequestBuilder = deleteByQueryRequestBuilder;
    }

    @Override
    public ActionRequest request() {
        return deleteByQueryRequestBuilder.request();
    }

    @Override
    public String explain() {
        try {
            SearchRequestBuilder source = deleteByQueryRequestBuilder.source();
            return source.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ActionResponse get() {

        return this.deleteByQueryRequestBuilder.get();
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return deleteByQueryRequestBuilder;
    }

}
