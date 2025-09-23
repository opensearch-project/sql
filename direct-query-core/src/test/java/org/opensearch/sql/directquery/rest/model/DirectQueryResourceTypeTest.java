/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class DirectQueryResourceTypeTest {

  @Test
  public void testFromStringValidValues() {
    assertEquals(DirectQueryResourceType.UNKNOWN, DirectQueryResourceType.fromString("unknown"));
    assertEquals(DirectQueryResourceType.UNKNOWN, DirectQueryResourceType.fromString("UNKNOWN"));
    assertEquals(DirectQueryResourceType.LABELS, DirectQueryResourceType.fromString("labels"));
    assertEquals(DirectQueryResourceType.LABELS, DirectQueryResourceType.fromString("LABELS"));
    assertEquals(DirectQueryResourceType.LABEL, DirectQueryResourceType.fromString("label"));
    assertEquals(DirectQueryResourceType.METADATA, DirectQueryResourceType.fromString("metadata"));
    assertEquals(DirectQueryResourceType.SERIES, DirectQueryResourceType.fromString("series"));
    assertEquals(DirectQueryResourceType.ALERTS, DirectQueryResourceType.fromString("alerts"));
    assertEquals(DirectQueryResourceType.RULES, DirectQueryResourceType.fromString("rules"));
    assertEquals(DirectQueryResourceType.ALERTMANAGER_ALERTS, 
        DirectQueryResourceType.fromString("alertmanager_alerts"));
    assertEquals(DirectQueryResourceType.ALERTMANAGER_ALERT_GROUPS, 
        DirectQueryResourceType.fromString("alertmanager_alert_groups"));
    assertEquals(DirectQueryResourceType.ALERTMANAGER_RECEIVERS, 
        DirectQueryResourceType.fromString("alertmanager_receivers"));
    assertEquals(DirectQueryResourceType.ALERTMANAGER_SILENCES, 
        DirectQueryResourceType.fromString("alertmanager_silences"));
  }

  @Test
  public void testFromStringMixedCase() {
    assertEquals(DirectQueryResourceType.LABELS, DirectQueryResourceType.fromString("LaBeLs"));
    assertEquals(DirectQueryResourceType.METADATA, DirectQueryResourceType.fromString("MetaData"));
  }

  @Test
  public void testFromStringNull() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      DirectQueryResourceType.fromString(null);
    });
    assertEquals("Resource type cannot be null", exception.getMessage());
  }

  @Test
  public void testFromStringInvalidValue() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      DirectQueryResourceType.fromString("invalid");
    });
    assertEquals("Invalid resource type: invalid", exception.getMessage());
  }

  @Test
  public void testFromStringEmptyString() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      DirectQueryResourceType.fromString("");
    });
    assertEquals("Invalid resource type: ", exception.getMessage());
  }

  @Test
  public void testFromStringWhitespace() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      DirectQueryResourceType.fromString("   ");
    });
    assertEquals("Invalid resource type:    ", exception.getMessage());
  }
}