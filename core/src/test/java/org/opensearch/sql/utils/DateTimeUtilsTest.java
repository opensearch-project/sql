/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.utils.DateTimeUtils.getRelativeZonedDateTime;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class DateTimeUtilsTest {
  @Test
  void round() {
    long actual =
        LocalDateTime.parse("2021-09-28T23:40:00")
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();
    long rounded = DateTimeUtils.roundFloor(actual, TimeUnit.HOURS.toMillis(1));
    assertEquals(
        LocalDateTime.parse("2021-09-28T23:00:00")
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());
  }

  @Test
  void testRelativeZonedDateTimeWithNow() {
    ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
    assertEquals(getRelativeZonedDateTime("now", now), now);
    assertEquals(getRelativeZonedDateTime("now()", now), now);
  }

  @Test
  void testRelativeZonedDateTimeWithSnap() {
    String dateTimeString = "2025-10-22 10:32:12";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
    ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d@d", zonedDateTime);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d-2h@h", zonedDateTime);
    assertEquals(snap1.toLocalDateTime().toString(), "2025-10-21T00:00");
    assertEquals(snap2.toLocalDateTime().toString(), "2025-10-19T08:00");
  }

  @Test
  void testRelativeZonedDateTimeWithOffset() {
    String dateTimeString = "2025-10-22 10:32:12";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
    ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d+1y@M", zonedDateTime);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d@d-2h+10m@h", zonedDateTime);
    assertEquals(snap1.toLocalDateTime().toString(), "2026-10-01T00:00");
    assertEquals(snap2.toLocalDateTime().toString(), "2025-10-18T22:00");
  }

  @Test
  void testRelativeZonedDateTimeWithWrongInput() {
    String dateTimeString = "2025-10-22 10:32:12";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
    ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> getRelativeZonedDateTime("1d+1y", zonedDateTime));
    assertEquals(e.getMessage(), "Wrong relative time expression: 1d+1y");
  }
}
