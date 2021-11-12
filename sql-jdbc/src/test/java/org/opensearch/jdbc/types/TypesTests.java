/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.JDBCType;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class TypesTests {

    @Test
    public void testNullTypeConverter() throws SQLException {
        TypeConverter tc = TypeConverters.getInstance(JDBCType.NULL);
        assertNull(tc.convert(null, Object.class, emptyMap()));
    }
}
