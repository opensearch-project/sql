/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal;

import java.sql.SQLException;
import java.sql.Wrapper;

public interface JdbcWrapper extends Wrapper {

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isInstance(this);
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString(), cce);
        }
    }
}
