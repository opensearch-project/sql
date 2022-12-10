/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jdbc;

import org.opensearch.jdbc.DatabaseMetaDataImpl.ResultSetColumnDescriptor;
import org.opensearch.jdbc.internal.results.ColumnMetaData;
import org.opensearch.jdbc.internal.results.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ResultSetMetaDataImpl}
 */
public class ResultSetMetaDataTests {

    private ResultSetMetaDataImpl metaData;

    @BeforeEach
    public void setUp() {
        ResultSetImpl resultSet = mock(ResultSetImpl.class);
        Schema schema = new Schema(Arrays.asList(
            new ColumnMetaData(new ResultSetColumnDescriptor("name", "keyword", null)),
            new ColumnMetaData(new ResultSetColumnDescriptor("address", "text", null)),
            new ColumnMetaData(new ResultSetColumnDescriptor("age", "long", null)),
            new ColumnMetaData(new ResultSetColumnDescriptor("balance", "float", null)),
            new ColumnMetaData(new ResultSetColumnDescriptor("employer", "nested", null)),
            new ColumnMetaData(new ResultSetColumnDescriptor("birthday", "timestamp", null))
        ));
        metaData = new ResultSetMetaDataImpl(resultSet, schema);
    }

    @Test
    public void getColumnTypeNameShouldReturnJDBCType() throws SQLException {
        assertEquals("VARCHAR", metaData.getColumnTypeName(1));
        assertEquals("VARCHAR", metaData.getColumnTypeName(2));
        assertEquals("BIGINT", metaData.getColumnTypeName(3));
        assertEquals("REAL", metaData.getColumnTypeName(4));
        assertEquals("STRUCT", metaData.getColumnTypeName(5));
        assertEquals("TIMESTAMP", metaData.getColumnTypeName(6));
    }

}
