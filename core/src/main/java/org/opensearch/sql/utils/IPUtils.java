/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv6.IPv6Address;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.exception.SemanticCheckException;

@UtilityClass
public class IPUtils {

  // Parameters for IP address strings.
  private static final IPAddressStringParameters.Builder commonValidationOptions =
      new IPAddressStringParameters.Builder()
          .allowEmpty(false)
          .allowMask(false)
          .setEmptyAsLoopback(false)
          .allowPrefixOnly(false)
          .allow_inet_aton(false)
          .allowSingleSegment(false);

  private static final IPAddressStringParameters ipAddressStringParameters =
      commonValidationOptions.allowPrefix(false).toParams();
  private static final IPAddressStringParameters ipAddressRangeStringParameters =
      commonValidationOptions.allowPrefix(true).toParams();

  /**
   * Builds and returns the {@link IPAddress} represented by the given IP address range string in
   * CIDR (classless inter-domain routing) notation. Returns {@link SemanticCheckException} if it
   * does not represent a valid IP address range. Supports both IPv4 and IPv6 address ranges.
   */
  public static IPAddress toRange(String s) throws SemanticCheckException {
    try {
      IPAddress range = new IPAddressString(s, ipAddressRangeStringParameters).toAddress();

      // Convert IPv6 mapped address range to IPv4.
      if (range.isIPv4Convertible()) {
        final int prefixLength = range.getPrefixLength();
        range = range.toIPv4().setPrefixLength(prefixLength, false);
      }

      return range;

    } catch (AddressStringException e) {
      final String errorFormat = "IP address range string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, s, e.getMessage()), e);
    }
  }

  /**
   * Builds and returns the {@link IPAddress} represented to the given IP address string. Throws
   * {@link SemanticCheckException} if it does not represent a valid IP address. Supports both IPv4
   * and IPv6 addresses.
   */
  public static IPAddress toAddress(String s) throws SemanticCheckException {
    try {
      IPAddress address = new IPAddressString(s, ipAddressStringParameters).toAddress();

      // Convert IPv6 mapped address to IPv4.
      if (address.isIPv4Convertible()) {
        address = address.toIPv4();
      }

      return address;
    } catch (AddressStringException e) {
      final String errorFormat = "IP address string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, s, e.getMessage()), e);
    }
  }

  /**
   * Compares the given {@link IPAddress} objects for order. Returns a negative integer, zero, or a
   * positive integer if the first {@link IPAddress} object is less than, equal to, or greater than
   * the second one. IPv4 addresses are mapped to IPv6 for comparison.
   */
  public static int compare(IPAddress a, IPAddress b) {
    final IPv6Address ipv6A = toIPv6Address(a);
    final IPv6Address ipv6B = toIPv6Address(b);

    return ipv6A.compareTo(ipv6B);
  }

  /** Returns the {@link IPv6Address} corresponding to the given {@link IPAddress}. */
  private static IPv6Address toIPv6Address(IPAddress ipAddress) {
    return ipAddress instanceof IPv4Address iPv4Address
        ? iPv4Address.toIPv6()
        : (IPv6Address) ipAddress;
  }
}
