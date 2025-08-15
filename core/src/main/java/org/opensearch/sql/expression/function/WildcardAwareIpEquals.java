/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import inet.ipaddr.IPAddressString;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.PPLFuncImpTable.FunctionImp2;

/**
 * Wildcard-aware IP address equals function that supports patterns like: - 192.168.*.* (matches any
 * IP starting with 192.168) - 192.168.1.* (matches 192.168.1.0 to 192.168.1.255) - 192.168.?.1
 * (matches 192.168.0.1 to 192.168.9.1)
 */
public class WildcardAwareIpEquals implements FunctionImp2 {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Check if this is an IP comparison with wildcards
    if (isIpType(arg1) && arg2.isA(SqlKind.LITERAL)) {
      String value = ((RexLiteral) arg2).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        // Convert wildcard IP pattern to range check
        return createIpRangeCheck(builder, arg1, value, false);
      }
    } else if (arg1.isA(SqlKind.LITERAL) && isIpType(arg2)) {
      String value = ((RexLiteral) arg1).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        // Handle reverse order (literal first)
        return createIpRangeCheck(builder, arg2, value, false);
      }
    }

    // Fall back to standard IP equals
    return builder.makeCall(PPLBuiltinOperators.EQUALS_IP, arg1, arg2);
  }

  @Override
  public PPLTypeChecker getTypeChecker() {
    // Accept IP type with IP or STRING (for wildcard patterns)
    return PPLTypeChecker.wrapUDT(List.of(
        List.of(ExprCoreType.IP, ExprCoreType.IP),
        List.of(ExprCoreType.IP, ExprCoreType.STRING)
    ));
  }

  protected boolean isIpType(RexNode node) {
    // Check if the node represents an IP type field
    String typeName = node.getType().getSqlTypeName().getName();
    return "IP".equals(typeName) || node.getType().toString().contains("IP");
  }

  protected boolean containsWildcards(String value) {
    return value.contains("*") || value.contains("?");
  }

  /**
   * Creates a range check for IP wildcard patterns. For example, 192.168.*.* becomes: ip >=
   * 192.168.0.0 AND ip <= 192.168.255.255
   */
  protected RexNode createIpRangeCheck(
      RexBuilder builder, RexNode ipField, String pattern, boolean negate) {
    try {
      // Parse the IP pattern and calculate range
      IpRange range = calculateIpRange(pattern);

      // Create range comparison: ip >= minIp AND ip <= maxIp
      RexNode minIpLiteral = builder.makeLiteral(range.min);
      RexNode maxIpLiteral = builder.makeLiteral(range.max);

      RexNode gteCheck = builder.makeCall(PPLBuiltinOperators.GTE_IP, ipField, minIpLiteral);
      RexNode lteCheck = builder.makeCall(PPLBuiltinOperators.LTE_IP, ipField, maxIpLiteral);

      RexNode rangeCheck = builder.makeCall(SqlStdOperatorTable.AND, gteCheck, lteCheck);

      // Negate if needed (for NOT_EQUALS)
      return negate ? builder.makeCall(SqlStdOperatorTable.NOT, rangeCheck) : rangeCheck;

    } catch (Exception e) {
      // If pattern parsing fails, fall back to standard comparison
      return builder.makeCall(
          negate ? PPLBuiltinOperators.NOT_EQUALS_IP : PPLBuiltinOperators.EQUALS_IP,
          ipField,
          builder.makeLiteral(pattern));
    }
  }

  /** Calculates the IP range for a wildcard pattern. */
  private IpRange calculateIpRange(String pattern) {
    String[] octets = pattern.split("\\.");
    if (octets.length != 4) {
      throw new IllegalArgumentException("Invalid IP pattern: " + pattern);
    }

    StringBuilder minIp = new StringBuilder();
    StringBuilder maxIp = new StringBuilder();

    for (int i = 0; i < 4; i++) {
      if (i > 0) {
        minIp.append(".");
        maxIp.append(".");
      }

      String octet = octets[i];
      if ("*".equals(octet)) {
        minIp.append("0");
        maxIp.append("255");
      } else if ("?".equals(octet)) {
        minIp.append("0");
        maxIp.append("9");
      } else if (octet.contains("*")) {
        // Handle patterns like "19*" -> 190-199
        String prefix = octet.substring(0, octet.indexOf('*'));
        minIp.append(prefix).append("0");
        maxIp.append(prefix).append("9");
      } else if (octet.contains("?")) {
        // Handle patterns like "19?" -> 190-199
        String prefix = octet.substring(0, octet.indexOf('?'));
        minIp.append(prefix).append("0");
        maxIp.append(prefix).append("9");
      } else {
        // Exact octet value
        minIp.append(octet);
        maxIp.append(octet);
      }
    }

    return new IpRange(minIp.toString(), maxIp.toString());
  }

  private static class IpRange {
    final String min;
    final String max;

    IpRange(String min, String max) {
      // Validate IP addresses
      IPAddressString minAddr = new IPAddressString(min);
      IPAddressString maxAddr = new IPAddressString(max);

      if (!minAddr.isValid() || !maxAddr.isValid()) {
        throw new IllegalArgumentException("Invalid IP range: " + min + " - " + max);
      }

      this.min = min;
      this.max = max;
    }
  }
}
