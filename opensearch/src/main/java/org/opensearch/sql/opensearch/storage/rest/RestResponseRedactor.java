/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Masks network identifiers in rest command cell values. Enabled per deployment via {@code
 * plugins.ppl.rest.redaction.enabled}; off by default.
 */
public final class RestResponseRedactor {

  private RestResponseRedactor() {}

  private static final String OCTET = "(25[0-5]|2[0-4]\\d|[0-1]?\\d\\d?)";
  private static final Pattern IPV4 =
      Pattern.compile("\\b" + OCTET + "\\." + OCTET + "\\." + OCTET + "\\." + OCTET + "\\b");
  private static final Pattern INET = Pattern.compile("inet\\[/[\\d.:]+\\]");
  private static final Pattern EC2_HOST =
      Pattern.compile("\\bip-" + OCTET + "-" + OCTET + "-" + OCTET + "-" + OCTET + "\\b");
  private static final Pattern IPV6 =
      Pattern.compile(
          "([0-9a-f]{1,4}:){7}[0-9a-f]{1,4}"
              + "|([0-9a-f]{1,4}(:[0-9a-f]{1,4})*)?::([0-9a-f]{1,4}(:[0-9a-f]{1,4})*)?",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AZ_NAME =
      Pattern.compile(
          "\\b[a-z]{2}(-(gov|iso[a-z]?))?-(central|(north|south)?(east|west)?)-\\d[a-z]\\b",
          Pattern.CASE_INSENSITIVE);

  private record Mask(Pattern pattern, String replacement) {}

  private static final List<Mask> MASKS =
      List.of(
          new Mask(IPV4, "x.x.x.x"),
          new Mask(INET, "inet[/x.x.x.x:y]"),
          new Mask(EC2_HOST, "<host>"),
          new Mask(IPV6, "x.x.x.x"),
          new Mask(AZ_NAME, "xx-xxxxx-xx"));

  /** Mask IPv4, inet, EC2 host names, IPv6, and availability-zone names in the text. */
  public static String redact(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    String out = text;
    for (Mask mask : MASKS) {
      out = mask.pattern().matcher(out).replaceAll(mask.replacement());
    }
    return out;
  }

  /** Mask availability-zone names only. */
  public static String maskAvailabilityZone(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    return AZ_NAME.matcher(text).replaceAll("xx-xxxxx-xx");
  }
}
