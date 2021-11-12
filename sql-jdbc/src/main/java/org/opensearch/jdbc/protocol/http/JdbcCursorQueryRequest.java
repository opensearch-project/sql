/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.JdbcQueryParam;;
import org.opensearch.jdbc.protocol.QueryRequest;


import java.util.List;
import java.util.Objects;

/**
 * Bean to encapsulate cursor ID
 *
 *  @author abbas hussain
 *  @since 07.05.20
 **/
public class JdbcCursorQueryRequest implements QueryRequest {

    String cursor;

    public JdbcCursorQueryRequest(String cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcCursorQueryRequest)) return false;
        JdbcCursorQueryRequest that = (JdbcCursorQueryRequest) o;
        return Objects.equals(cursor, that.cursor) &&
                Objects.equals(getParameters(), that.getParameters());
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, getParameters());
    }

    @Override
    public String getQuery() {
        return cursor;
    }

    @Override
    public List<JdbcQueryParam> getParameters() {
        return null;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public String toString() {
        return "JdbcQueryRequest{" +
                "cursor='" + cursor + '\'' +
                '}';
    }
}
