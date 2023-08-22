/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

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
  void getQueryStartClock_differs_from_instantNow() throws InterruptedException {
    // Give system clock a chance to advance.
    Thread.sleep(1000);
    assertNotEquals(Instant.now(), functionProperties.getQueryStartClock().instant());
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
    assertNotEquals(0, output.size());
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

  @TestFactory
  Stream<DynamicTest> functionProperties_none_throws_on_access() {
    Consumer<Executable> tb =
        tc -> {
          RuntimeException e = assertThrows(FunctionProperties.UnexpectedCallException.class, tc);
          assertEquals(
              "FunctionProperties.None is a null object and not meant to be accessed.",
              e.getMessage());
        };
    return Stream.of(
        DynamicTest.dynamicTest(
            "getQueryStartClock", () -> tb.accept(FunctionProperties.None::getQueryStartClock)),
        DynamicTest.dynamicTest(
            "getSystemClock", () -> tb.accept(FunctionProperties.None::getSystemClock)));
  }
}
