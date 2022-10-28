/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

class FunctionDSLnullMissingHandlingTest {

  public static final ExprNullValue NULL = ExprNullValue.of();
  private static final ExprValue ANY = new ExprValue() {
    @Override
    public Object value() {
      throw new RuntimeException();
    }

    @Override
    public ExprType type() {
      throw new RuntimeException();
    }

    @Override
    public int compareTo(ExprValue o) {
      throw new RuntimeException();
    }
  };
  public static final ExprMissingValue MISSING = ExprMissingValue.of();
  private final SerializableFunction<ExprValue, ExprValue> oneArg =
      v -> ANY;

  private final SerializableBiFunction<ExprValue, ExprValue, ExprValue> twoArgs =
      (v1, v2) -> ANY;

  private final SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue> threeArgs =
      (v1, v2, v3) -> ANY;

  @Test
  void nullMissingHandling_oneArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(oneArg).apply(NULL));
  }

  @Test
  void nullMissingHandling_oneArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(oneArg).apply(MISSING));
  }

  @Test
  void nullMissingHandling_oneArg_apply() {
    assertEquals(ANY, nullMissingHandling(oneArg).apply(ANY));
  }

  @Test
  void nullMissingHandling_twoArgs_firstArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(twoArgs).apply(NULL, ANY));
  }

  @Test
  void nullMissingHandling_twoArgs_secondArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(twoArgs).apply(ANY, NULL));
  }


  @Test
  void nullMissingHandling_twoArgs_firstArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(twoArgs).apply(MISSING, ANY));
  }

  @Test
  void nullMissingHandling_twoArgs_secondArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(twoArgs).apply(ANY, MISSING));
  }

  @Test
  void nullMissingHandling_twoArgs_apply() {
    assertEquals(ANY, nullMissingHandling(twoArgs).apply(ANY, ANY));
  }


  @Test
  void nullMissingHandling_threeArgs_firstArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(threeArgs).apply(NULL, ANY, ANY));
  }

  @Test
  void nullMissingHandling_threeArgs_secondArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(threeArgs).apply(ANY, NULL, ANY));
  }

  @Test
  void nullMissingHandling_threeArgs_thirdArg_nullValue() {
    assertEquals(NULL, nullMissingHandling(threeArgs).apply(ANY, ANY, NULL));
  }


  @Test
  void nullMissingHandling_threeArgs_firstArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(threeArgs).apply(MISSING, ANY, ANY));
  }

  @Test
  void nullMissingHandling_threeArg_secondArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(threeArgs).apply(ANY, MISSING, ANY));
  }

  @Test
  void nullMissingHandling_threeArg_thirdArg_missingValue() {
    assertEquals(MISSING, nullMissingHandling(threeArgs).apply(ANY, ANY, MISSING));
  }

  @Test
  void nullMissingHandling_threeArg_apply() {
    assertEquals(ANY, nullMissingHandling(threeArgs).apply(ANY, ANY, ANY));
  }
}
