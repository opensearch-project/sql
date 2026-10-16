/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/**
 * The type of a column that reads another field than the index field of its name replaces that
 * field's type, date formats included; an extended type of the same core type does not.
 */
class OpenSearchExprValueFactoryColumnTypesTest {

  private static final Map<String, OpenSearchDataType> MAPPING =
      OpenSearchDataType.parseMapping(
          Map.of(
              "ts", Map.of("type", "date"),
              "d2", Map.of("type", "date", "format", "epoch_second")));

  /** {@code eval d2 = ts}: d2 holds the value of ts, epoch milliseconds. */
  @Test
  void columnTypesReplaceTheTypeOfTheIndexFieldOfTheirName() {
    OpenSearchDataType ts = MAPPING.get("ts");
    long millis = Instant.parse("2020-01-02T03:04:05.123Z").toEpochMilli();

    OpenSearchExprValueFactory extended = new OpenSearchExprValueFactory(MAPPING, true);
    extended.extendTypeMapping(Map.of("d2", ts));
    // Both are timestamps, so d2 keeps its epoch_second format and reads the year 51972.
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochSecond(millis)),
        extended.construct("d2", millis, true));

    OpenSearchExprValueFactory replaced = new OpenSearchExprValueFactory(MAPPING, true);
    replaced.putColumnTypes(Map.of("d2", ts));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(millis)),
        replaced.construct("d2", millis, true));
  }
}
