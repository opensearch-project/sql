package org.opensearch.sql.expression.ip;

import com.google.common.net.InetAddresses;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ToString
@EqualsAndHashCode(callSuper = false)
public class CidrExpression extends FunctionExpression {

    private final Expression addressExpression;
    private final InetAddressRange range;

    public CidrExpression(List<Expression> arguments) {
        super(FunctionName.of("cidr"), arguments);

        // Must be exactly two arguments.
        if (arguments.size() != 2) {
            String msg = String.format("Unexpected number of arguments to function '%s'. Expected %s, but found %s.", FunctionName.of("cidr"), 2, arguments.size());
            throw new ExpressionEvaluationException(msg);
        }

        this.addressExpression = arguments.getFirst();
        this.range = new InetAddressRange(arguments.getLast().valueOf().stringValue());
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        ExprValue addressValue = addressExpression.valueOf(valueEnv);
        if (addressValue.isNull() || addressValue.isMissing())
            return ExprValueUtils.nullValue();

        String addressString = addressValue.stringValue();
        if (!InetAddresses.isInetAddress(addressString))
            return ExprValueUtils.nullValue();

        InetAddress address = InetAddresses.forString(addressString);
        return ExprValueUtils.booleanValue(range.contains(address));
    }

    @Override
    public ExprType type() {
        return ExprCoreType.BOOLEAN;
    }

    /**
     * Represents an IP address range.
     * Supports both IPv4 and IPv6 addresses.
     */
    private class InetAddressRange implements Serializable {

        // Basic CIDR notation pattern.
        private static final Pattern cidrPattern = Pattern.compile("(?<address>.+)[/](?<prefix>[0-9]+)");

        // Lower/upper bounds for the IP address range.
        private final InetAddress lowerBound;
        private final InetAddress upperBound;

        /**
         * Builds a new IP address range from the given CIDR notation string.
         *
         * @param cidr CIDR notation string (e.g. "198.51.100.0/24" or "2001:0db8::/32")
         */
        public InetAddressRange(String cidr) {

            // Parse address and network length.
            Matcher cidrMatcher = cidrPattern.matcher(cidr);
            if (!cidrMatcher.matches())
                throw new SemanticCheckException(String.format("CIDR notation '%s' in not valid", range));

            String addressString = cidrMatcher.group("address");
            if (!InetAddresses.isInetAddress(addressString))
                throw new SemanticCheckException(String.format("IP address '%s' in not valid", addressString));

            InetAddress address = InetAddresses.forString(addressString);

            int networkLengthBits = Integer.parseInt(cidrMatcher.group("prefix"));
            int addressLengthBits = address.getAddress().length * Byte.SIZE;

            if (networkLengthBits > addressLengthBits)
                throw new SemanticCheckException(String.format("Network length of '%s' bits is not valid", networkLengthBits));

            // Build bounds by converting the address to an integer, setting all the non-significant bits to
            // zero for the lower bounds and one for the upper bounds, and then converting back to addresses.
            BigInteger lowerBoundInt = InetAddresses.toBigInteger(address);
            BigInteger upperBoundInt = InetAddresses.toBigInteger(address);

            int hostLengthBits = addressLengthBits - networkLengthBits;
            for (int bit = 0; bit < hostLengthBits; bit++) {
                lowerBoundInt = lowerBoundInt.clearBit(bit);
                upperBoundInt = upperBoundInt.setBit(bit);
            }

            if (address instanceof Inet4Address) {
                lowerBound = InetAddresses.fromIPv4BigInteger(lowerBoundInt);
                upperBound = InetAddresses.fromIPv4BigInteger(upperBoundInt);
            } else {
                lowerBound = InetAddresses.fromIPv6BigInteger(lowerBoundInt);
                upperBound = InetAddresses.fromIPv6BigInteger(upperBoundInt);
            }
        }

        /**
         * Returns whether the IP address is contained within the range.
         *
         * @param address IPv4 or IPv6 address, represented as a {@link BigInteger}.
         *                (see {@link InetAddresses#toBigInteger(InetAddress)}).
         */
        public boolean contains(InetAddress address) {

            if ((address instanceof Inet4Address) ^ (lowerBound instanceof Inet4Address)) return false;

            BigInteger addressInt = InetAddresses.toBigInteger(address);

            if (addressInt.compareTo(InetAddresses.toBigInteger(lowerBound)) < 0) return false;
            if (addressInt.compareTo(InetAddresses.toBigInteger(upperBound)) <= 0) return false;

            return true;
        }
    }
}
