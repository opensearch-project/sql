/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.time.*;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.*;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.RangeQuery.Comparison;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RangeQueryTest {

  @Test
  void should_throw_exception_for_unsupported_comparison() {
    // Note that since we do switch check on enum comparison, this should be impossible
    assertThrows(
        IllegalStateException.class,
        () ->
            new RangeQuery(Comparison.BETWEEN)
                .doBuild("name", STRING, ExprValueUtils.stringValue("John")));
  }

  @Test
  void test_timestamp_with_no_format() {
    OpenSearchDateType openSearchDateType = OpenSearchDateType.of(ExprCoreType.TIMESTAMP);
    assertNotNull(
        new RangeQuery(Comparison.LT)
            .doBuild("time", openSearchDateType, new ExprTimestampValue("2021-11-08 17:00:00")));
  }

  @Test
  void test_timestamp_has_format() {
    String timestamp = "2019-03-23 21:34:46";
    OpenSearchDateType dateType = OpenSearchDateType.of("yyyy-MM-dd HH:mm:ss");
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);
    ExprValue literal = ExprValueUtils.timestampValue(zonedDateTime.toInstant());
    assertNotNull(new RangeQuery(Comparison.LT).doBuild("time_stamp", dateType, literal));
  }

  @Test
  void test_time_with_no_format() {
    OpenSearchDateType openSearchDateType = OpenSearchDateType.of(ExprCoreType.TIME);
    assertNotNull(
        new RangeQuery(Comparison.LT)
            .doBuild("time", openSearchDateType, new ExprTimeValue("17:00:00")));
  }

  @Test
  void test_time_has_format() {
    long epochTimestamp = 1636390800000L; // Corresponds to "2021-11-08T17:00:00Z"
    String format = "epoch_millis";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(String.valueOf(epochTimestamp));
    ExprValue literal = ExprValueUtils.timeValue(zonedDateTime.toLocalTime());
    assertNotNull(new RangeQuery(Comparison.LT).doBuild("time", dateType, literal));
  }

  @Test
  void test_date_with_no_format() {
    OpenSearchDateType openSearchDateType = OpenSearchDateType.of(ExprCoreType.DATE);
    assertNotNull(
        new RangeQuery(Comparison.LT)
            .doBuild("date", openSearchDateType, new ExprDateValue("2021-11-08")));
  }

  @Test
  void test_date_has_format() {
    String dateString = "2021-11-08";
    String format = "yyyy-MM-dd";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    LocalDate parsedDate = dateType.getParsedDateTime(dateString).toLocalDate();
    ExprValue literal = ExprValueUtils.dateValue(parsedDate);
    assertNotNull(new RangeQuery(Comparison.LT).doBuild("date", dateType, literal));
  }

  @Test
  void test_non_date_field_type() {
    String dateString = "2021-11-08";
    OpenSearchDateType dateType = OpenSearchDateType.of(STRING);
    ExprValue literal = ExprValueUtils.stringValue(dateString);
    assertNotNull(new RangeQuery(Comparison.LT).doBuild("string_value", dateType, literal));
  }
}
