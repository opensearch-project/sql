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

package org.opensearch.jdbc.test.mocks;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MockResultSet {

    MockResultSetMetaData mockResultSetMetaData;
    MockResultSetRows mockResultSetRows;

    public MockResultSet(MockResultSetMetaData mockResultSetMetaData, MockResultSetRows mockResultSetRows) {
        if (mockResultSetMetaData == null || mockResultSetRows == null) {
            throw new IllegalArgumentException("Neither metadata nor rows can be null");
        }

        if (!mockResultSetRows.isEmpty() && mockResultSetMetaData.getColumnCount() != mockResultSetRows.getColumnCount()) {
            throw new IllegalArgumentException(
                    "Column count mismatch. MetaData has " + mockResultSetMetaData.getColumnCount() +
                            " columns, but rows have " + mockResultSetRows.getColumnCount());
        }

        this.mockResultSetMetaData = mockResultSetMetaData;
        this.mockResultSetRows = mockResultSetRows;
    }

    public void assertMatches(ResultSet rs) throws SQLException {
        mockResultSetMetaData.assertMatches(rs.getMetaData());
        mockResultSetRows.assertMatches(rs);
    }
}
