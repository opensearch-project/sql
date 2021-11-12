/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.types;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.opensearch.jdbc.test.UTCTimeZoneTestExtension;
import java.sql.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(UTCTimeZoneTestExtension.class)
public class TimeTypeTest {

  @ParameterizedTest
  @CsvSource(value = {
      "00:00:00, 00:00:00",
      "01:01:01, 01:01:01",
      "23:59:59, 23:59:59"
  })
  void testTimeFromString(String inputString, String resultString) {
    Time time = Assertions.assertDoesNotThrow(
        () -> TimeType.INSTANCE.fromValue(inputString, null));
    assertEquals(resultString, time.toString());
  }
}
