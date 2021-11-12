/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.Parameter;
import org.opensearch.jdbc.protocol.QueryRequest;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class JsonQueryRequest implements QueryRequest {

    private String query;
    private int fetchSize;
    private List<? extends Parameter> parameters;

    public JsonQueryRequest(QueryRequest queryRequest) {
        this.query = queryRequest.getQuery();
        this.parameters = queryRequest.getParameters();
        this.fetchSize = queryRequest.getFetchSize();

    }

    @Override
    public String getQuery() {
        return query;
    }

    @JsonInclude(Include.NON_NULL)
    @Override
    public List<? extends Parameter> getParameters() {
        return parameters;
    }

    @JsonProperty("fetch_size")
    @Override
    public int getFetchSize() {
        return fetchSize;
    }
}
