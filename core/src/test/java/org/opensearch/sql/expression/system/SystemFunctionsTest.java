/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprIntervalValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;

public class SystemFunctionsTest {

  @Test
  void typeof() {
    assertEquals(STRING, DSL.typeof(DSL.literal(1)).type());

    assertEquals("NESTED", typeofGetValue(new ExprCollectionValue(List.of())));
    assertEquals("BOOLEAN", typeofGetValue(ExprBooleanValue.of(false)));
    assertEquals("BYTE", typeofGetValue(new ExprByteValue(0)));
    assertEquals("DATE", typeofGetValue(new ExprDateValue(LocalDate.now())));
    assertEquals("DOUBLE", typeofGetValue(new ExprDoubleValue(0)));
    assertEquals("FLOAT", typeofGetValue(new ExprFloatValue(0)));
    assertEquals("INTEGER", typeofGetValue(new ExprIntegerValue(0)));
    assertEquals("INTERVAL", typeofGetValue(new ExprIntervalValue(Duration.ofDays(0))));
    assertEquals("LONG", typeofGetValue(new ExprLongValue(0)));
    assertEquals("SHORT", typeofGetValue(new ExprShortValue(0)));
    assertEquals("KEYWORD", typeofGetValue(new ExprStringValue("")));
    assertEquals("OBJECT", typeofGetValue(new ExprTupleValue(new LinkedHashMap<>())));
    assertEquals("TIME", typeofGetValue(new ExprTimeValue(LocalTime.now())));
    assertEquals("TIMESTAMP", typeofGetValue(new ExprTimestampValue(Instant.now())));
    assertEquals("UNDEFINED", typeofGetValue(ExprNullValue.of()));
    assertEquals("UNDEFINED", typeofGetValue(ExprMissingValue.of()));
    assertEquals(
        "UNKNOWN",
        typeofGetValue(
            new AbstractExprValue() {
              @Override
              public int compare(ExprValue other) {
                return 0;
              }

              @Override
              public boolean equal(ExprValue other) {
                return false;
              }

              @Override
              public Object value() {
                return null;
              }

              @Override
              public ExprType type() {
                return ExprCoreType.UNKNOWN;
              }
            }));
  }

  private String typeofGetValue(ExprValue input) {
    return DSL.typeof(DSL.literal(input)).valueOf().stringValue();
  }
}
