package org.opensearch.sql.utils;

import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;

public class IPUtils {

    /**
     * Returns whether the given IP address is within the specified IP address range.
     * Supports both IPv4 and IPv6 addresses.
     *
     * @param address IP address (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
     * @param range IP address range in CIDR notation (e.g. "198.51.100.0/24" or "2001:0db8:/32")
     * @return true value if IP address is in range; else false value.
     */
    public static ExprBooleanValue isAddressInRange(ExprValue address, ExprValue range) {

        // TODO - implementation
        return ExprBooleanValue.of(true);
    }
}
