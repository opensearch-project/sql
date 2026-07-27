/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * The sensitivity class of a {@link Column}'s values, the join key for {@code rest} response
 * redaction. A field is classified ONCE on its column; the platform decides how (or whether) to
 * mask each class, so the same masking is reused across every endpoint that declares a column of
 * that class, endpoint- and column-name-agnostic.
 *
 * <p>Closed and core-owned (never extended by a plugin) so the set of maskable classes is
 * auditable. {@link #NONE} is the default and means the value is never redacted.
 */
public enum RedactionClass {
  /** Not sensitive; never redacted (the default for a column). */
  NONE,
  /** An IP address (IPv4, IPv6, or an {@code inet[/...]} form). */
  IP,
  /** A host name (for example an EC2 style {@code ip-a-b-c-d} name). */
  HOSTNAME,
  /** An availability-zone name (for example {@code us-east-1a}). */
  AVAILABILITY_ZONE,
  /** Free text that may embed any of the above network identifiers. */
  NETWORK_TEXT
}
