/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import java.sql.SQLException;

public abstract class NumberType<T> implements TypeHelper<T> {

    /**
     * Returns a {@link Number} type value as a {@link Double} if it falls
     * within a specified min and max range.
     *
     * @param value Number value to parse
     * @param minValue minimum value possible
     * @param maxValue maximum value possible
     *
     * @return The double value corresponding to the specified Number if
     *         it falls within the specified min and max range.
     *
     * @throws SQLException If the double value is outside the possible
     *         range specified
     */
    double getDoubleValueWithinBounds(Number value, double minValue, double maxValue) throws SQLException {
        double doubleValue = value.doubleValue();

        if (roundOffValue())
            doubleValue = Math.round(doubleValue);

        if (doubleValue > maxValue || doubleValue < minValue)
            throw valueOutOfRangeException(value);

        return doubleValue;
    }

    /**
     * Whether to round off a fractional value during cross conversion
     * from a different type to this type.
     *
     * @return true, to apply rounding off, false otherwise
     */
    public boolean roundOffValue() {
        return true;
    }
}
