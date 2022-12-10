/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.Parameter;
import org.opensearch.jdbc.protocol.QueryRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Definition of json cursor request
 *
 *  @author abbas hussain
 *  @since 07.05.20
 **/
public class JsonCursorQueryRequest implements QueryRequest  {

    private final String cursor;

    public JsonCursorQueryRequest(QueryRequest queryRequest) {
        this.cursor = queryRequest.getQuery();
    }

    @JsonProperty("cursor")
    @Override
    public String getQuery() {
        return cursor;
    }

    @JsonIgnore
    @Override
    public List<? extends Parameter> getParameters() {
        return null;
    }

    @JsonIgnore
    @Override
    public int getFetchSize() {
        return 0;
    }
}
