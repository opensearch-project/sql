/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class DateTimeUtilsTest {
  @Test
  void round() {
    long actual = LocalDateTime.parse("2021-09-28T23:40:00").atZone(ZoneId.systemDefault())
        .toInstant().toEpochMilli();
    long rounded = DateTimeUtils.roundFloor(actual, TimeUnit.HOURS.toMillis(1));
    assertEquals(
        LocalDateTime.parse("2021-09-28T23:00:00").atZone(ZoneId.systemDefault()).toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());
  }
}
