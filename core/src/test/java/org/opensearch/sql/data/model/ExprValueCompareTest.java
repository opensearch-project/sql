/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;

import org.junit.jupiter.api.Test;

public class ExprValueCompareTest {

  @Test
  public void timeValueCompare() {
    assertEquals(0, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(1, new ExprTimeValue("19:00:00").compareTo(new ExprTimeValue("18:00:00")));
    assertEquals(-1, new ExprTimeValue("18:00:00").compareTo(new ExprTimeValue("19:00:00")));
  }

  @Test
  public void dateValueCompare() {
    assertEquals(0, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(1, new ExprDateValue("2012-08-08").compareTo(new ExprDateValue("2012-08-07")));
    assertEquals(-1, new ExprDateValue("2012-08-07").compareTo(new ExprDateValue("2012-08-08")));
  }

  @Test
  public void datetimeValueCompare() {
    assertEquals(0,
        new ExprDatetimeValue("2012-08-07 18:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 18:00:00")));
    assertEquals(1,
        new ExprDatetimeValue("2012-08-07 19:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 18:00:00")));
    assertEquals(-1,
        new ExprDatetimeValue("2012-08-07 18:00:00")
            .compareTo(new ExprDatetimeValue("2012-08-07 19:00:00")));
  }

  @Test
  public void timestampValueCompare() {
    assertEquals(0,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(1,
        new ExprTimestampValue("2012-08-07 19:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 18:00:00")));
    assertEquals(-1,
        new ExprTimestampValue("2012-08-07 18:00:00")
            .compareTo(new ExprTimestampValue("2012-08-07 19:00:00")));
  }

  @Test
  public void missingCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_MISSING.compareTo(LITERAL_FALSE));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_FALSE.compareTo(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> ExprMissingValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with MISSING is undefined",
        exception.getMessage());
  }

  @Test
  public void nullCompareToMethodShouldNotBeenCalledDirectly() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_NULL.compareTo(LITERAL_FALSE));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> LITERAL_FALSE.compareTo(LITERAL_NULL));
    assertEquals("[BUG] Unreachable, Comparing with NULL or MISSING is undefined",
        exception.getMessage());

    exception = assertThrows(IllegalStateException.class,
        () -> ExprNullValue.of().compare(LITERAL_MISSING));
    assertEquals("[BUG] Unreachable, Comparing with NULL is undefined",
        exception.getMessage());
  }
}
