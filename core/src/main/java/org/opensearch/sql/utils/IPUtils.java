package org.opensearch.sql.utils;

import com.google.common.net.InetAddresses;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;

import java.util.Arrays;

public class IPUtils {

    /**
     * Returns whether the given IP address is within the specified IP address range.
     * Supports both IPv4 and IPv6 addresses.
     *
     * @param addressExprValue IP address (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
     * @param rangeExprValue IP address range in CIDR notation (e.g. "198.51.100.0/24" or "2001:0db8:/32")
     * @return true if IP address is in range; else false
     */
    public static ExprBooleanValue isAddressInRange(ExprValue addressExprValue, ExprValue rangeExprValue) {

        byte[] addressBytes;
        try {
            addressBytes = InetAddresses.forString(addressExprValue.stringValue()).getAddress();
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Argument '%s' is not a valid IP address", addressExprValue));
        }

        int prefixLengthBytes;
        byte[] rangeBytes;
        try {
            String[] rangeFields = rangeExprValue.stringValue().split("/");
            prefixLengthBytes = Integer.parseInt(rangeFields[1]) / Byte.SIZE;
            rangeBytes = Arrays.copyOfRange(InetAddresses.forString(rangeFields[0]).getAddress(), 0, prefixLengthBytes);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Argument '%s' is not a valid IP address range in CIDR notation", rangeExprValue));
        }

        if(addressBytes.length < prefixLengthBytes)
            return ExprBooleanValue.of(false);

        return ExprBooleanValue.of(Arrays.equals(addressBytes, 0, prefixLengthBytes, rangeBytes, 0, prefixLengthBytes));
    }
}
