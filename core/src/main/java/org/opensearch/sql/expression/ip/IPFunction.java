package org.opensearch.sql.expression.ip;

import com.google.common.net.InetAddresses;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.sql.data.type.ExprCoreType.*;
import static org.opensearch.sql.expression.function.FunctionDSL.*;

/**
 * Utility class that defines and registers IP functions.
 */
@UtilityClass
public class IPFunction {

    private static final Pattern cidrPattern = Pattern.compile("(?<address>.+)[/](?<networkLength>[0-9]+)");

    public void register(BuiltinFunctionRepository repository) {
        repository.register(cidr());
    }

    private DefaultFunctionResolver cidr() {
        return define(
                BuiltinFunctionName.CIDR.getName(),
                impl(nullMissingHandling(IPFunction::exprCidr), BOOLEAN, STRING, STRING));
    }

    /**
     * Returns whether the given IP address is within the specified IP address range.
     * Supports both IPv4 and IPv6 addresses.
     *
     * @param addressExprValue IP address (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
     * @param rangeExprValue   IP address range in CIDR notation (e.g. "198.51.100.0/24" or "2001:0db8::/32")
     * @return null if the address is not valid; true if the address is in the range; otherwise false.
     * @throws SemanticCheckException if the range is not valid
     */
    private ExprValue exprCidr(ExprValue addressExprValue, ExprValue rangeExprValue) {

        // Get address
        String addressString = addressExprValue.stringValue();
        if (!InetAddresses.isInetAddress(addressString))
            return ExprValueUtils.nullValue();

        InetAddress address = InetAddresses.forString(addressString);

        // Get range and network length
        String rangeString = rangeExprValue.stringValue();

        Matcher cidrMatcher = cidrPattern.matcher(rangeString);
        if (!cidrMatcher.matches())
            throw new SemanticCheckException(String.format("CIDR notation '%s' in not valid", rangeString));

        String rangeAddressString = cidrMatcher.group("address");
        if (!InetAddresses.isInetAddress(rangeAddressString))
            throw new SemanticCheckException(String.format("IP address '%s' in not valid", rangeAddressString));

        InetAddress rangeAddress = InetAddresses.forString(rangeAddressString);

        if ((address instanceof Inet4Address) ^ (rangeAddress instanceof Inet4Address))
            return ExprValueUtils.booleanValue(false);

        int networkLengthBits = Integer.parseInt(cidrMatcher.group("networkLength"));
        int addressLengthBits = address.getAddress().length * Byte.SIZE;

        if (networkLengthBits > addressLengthBits)
            throw new SemanticCheckException(String.format("Network length of '%s' bits is not valid", networkLengthBits));

        // Build bounds by converting the address to an integer, setting all the non-significant bits to
        // zero for the lower bounds and one for the upper bounds, and then converting back to addresses.
        BigInteger lowerBoundInt = InetAddresses.toBigInteger(rangeAddress);
        BigInteger upperBoundInt = InetAddresses.toBigInteger(rangeAddress);

        int hostLengthBits = addressLengthBits - networkLengthBits;
        for (int bit = 0; bit < hostLengthBits; bit++) {
            lowerBoundInt = lowerBoundInt.clearBit(bit);
            upperBoundInt = upperBoundInt.setBit(bit);
        }

        // Convert the address to an integer and compare it to the bounds.
        BigInteger addressInt = InetAddresses.toBigInteger(address);
        return ExprValueUtils.booleanValue((addressInt.compareTo(lowerBoundInt) >= 0) && (addressInt.compareTo(upperBoundInt) <= 0));
    }
}
