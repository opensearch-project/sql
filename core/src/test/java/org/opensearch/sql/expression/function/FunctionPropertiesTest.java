/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FunctionPropertiesTest {

  FunctionProperties functionProperties;
  Instant startTime;

  @BeforeEach
  void init() {
    startTime = Instant.now();
    functionProperties = new FunctionProperties(startTime, ZoneId.systemDefault());
  }

  @Test
  void getQueryStartClock_returns_constructor_instant() {
    assertEquals(startTime, functionProperties.getQueryStartClock().instant());
  }

  @Test
  void getQueryStartClock_differs_from_instantNow() {
    assertNotEquals(Instant.now(), functionProperties.getQueryStartClock());
  }

  @Test
  void getSystemClock_is_systemClock() {
    assertEquals(Clock.systemDefaultZone(), functionProperties.getSystemClock());
  }

  @Test
  void functionProperties_can_be_serialized() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(functionProperties);
    objectOutput.flush();
  }

  @Test
  void functionProperties_can_be_deserialized() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(functionProperties);
    objectOutput.flush();
    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    ObjectInputStream objectInput = new ObjectInputStream(input);
    assertEquals(functionProperties, objectInput.readObject());
  }
}
