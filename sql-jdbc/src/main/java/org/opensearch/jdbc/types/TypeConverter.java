/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Map;

public interface TypeConverter {

    /**
     * This method allows retrieving a column value as an instance of
     * a different class than the default Java class that the column's
     * JDBCType maps to.
     * <p>
     * This implements the aspect of the JDBC spec that specifies
     * multiple JDBCTypes on which a ResultSet getter method may be called.
     *
     * @param <T> Type of the Java Class
     * @param value Column value
     * @param clazz Instance of the Class to which the value needs to be
     *         converted
     * @param conversionParams Optional conversion parameters to use in
     *         the conversion
     *
     * @return Column value as an instance of type T
     *
     * @throws SQLException if the conversion is not supported or the
     *         conversion operation fails.
     */
    <T> T convert(Object value, Class<T> clazz, Map<String, Object> conversionParams) throws SQLException;

    default SQLDataException objectConversionException(Object value, Class clazz) {
        return new SQLDataException(String.format(
                "Can not convert object '%s' of type '%s' to type '%s'",
                value, value.getClass().getName(), clazz.getName()));
    }
}
