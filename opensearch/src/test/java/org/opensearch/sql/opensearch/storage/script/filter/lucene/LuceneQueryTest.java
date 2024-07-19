/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LuceneQueryTest {

  @Test
  void should_not_support_single_argument_by_default() {
    assertFalse(new LuceneQuery() {}.canSupport(DSL.abs(ref("age", INTEGER))));
  }

  @Test
  void should_throw_exception_if_not_implemented() {
    assertThrows(
        UnsupportedOperationException.class, () -> new LuceneQuery() {}.doBuild(null, null, null));
  }

  @Test
  void should_cast_to_time_with_format() {
    String format = "HH:mm:ss.SSS || HH:mm:ss";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(ref("time_value", dateType), DSL.castTime(literal("17:00:00")))));
  }

  @Test
  void should_cast_to_time_with_no_format() {
    String format = "HH:mm";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(ref("time_value", dateType), DSL.castTime(literal("17:00:00")))));
  }

  @Test
  void should_cast_to_date_with_format() {
    String format = "yyyy-MM-dd";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(ref("date_value", dateType), DSL.castDate(literal("2017-01-02")))));
  }

  @Test
  void should_cast_to_date_with_no_format() {
    String format = "yyyy/MM/dd";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(ref("date_value", dateType), DSL.castDate(literal("2017-01-02")))));
  }

  @Test
  void should_cast_to_timestamp_with_format() {
    String format = "yyyy-MM-dd HH:mm:ss";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(
                    ref("timestamp_value", dateType),
                    DSL.castTimestamp(literal("2021-11-08 17:00:00")))));
  }

  @Test
  void should_cast_to_timestamp_with_no_format() {
    String format = "2021/11/08T17:00:00Z";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertThrows(
        SemanticCheckException.class,
        () ->
            new LuceneQuery() {}.build(
                DSL.equal(
                    ref("timestamp_value", dateType),
                    DSL.castTimestamp(literal("2021-11-08 17:00:00 ")))));
  }
}
