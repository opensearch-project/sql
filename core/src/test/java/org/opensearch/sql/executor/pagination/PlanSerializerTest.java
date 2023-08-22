/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.utils.TestOperator;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PlanSerializerTest {

  StorageEngine storageEngine;

  PlanSerializer planCache;

  @BeforeEach
  void setUp() {
    storageEngine = mock(StorageEngine.class);
    planCache = new PlanSerializer(storageEngine);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "pewpew",
        "asdkfhashdfjkgakgfwuigfaijkb",
        "ajdhfgajklghadfjkhgjkadhgad"
            + "kadfhgadhjgfjklahdgqheygvskjfbvgsdklgfuirehiluANUIfgauighbahfuasdlhfnhaughsdlfhaughaggf"
            + "and_some_other_funny_stuff_which_could_be_generated_while_sleeping_on_the_keyboard"
      })
  void serialize_deserialize_str(String input) {
    var compressed = serialize(input);
    assertEquals(input, deserialize(compressed));
    if (input.length() > 200) {
      // Compression of short strings isn't profitable, because encoding into string and gzip
      // headers add more bytes than input string has.
      assertTrue(compressed.length() < input.length());
    }
  }

  public static class SerializableTestClass implements Serializable {
    public int field;

    @Override
    public boolean equals(Object obj) {
      return field == ((SerializableTestClass) obj).field;
    }
  }

  // Can't serialize private classes because they are not accessible
  private class NotSerializableTestClass implements Serializable {
    public int field;

    @Override
    public boolean equals(Object obj) {
      return field == ((SerializableTestClass) obj).field;
    }
  }

  @Test
  void serialize_deserialize_obj() {
    var obj = new SerializableTestClass();
    obj.field = 42;
    assertEquals(obj, deserialize(serialize(obj)));
    assertNotSame(obj, deserialize(serialize(obj)));
  }

  @Test
  void serialize_throws() {
    assertThrows(Throwable.class, () -> serialize(new NotSerializableTestClass()));
    var testObj = new TestOperator();
    testObj.setThrowIoOnWrite(true);
    assertThrows(Throwable.class, () -> serialize(testObj));
  }

  @Test
  void deserialize_throws() {
    assertAll(
        // from gzip - damaged header
        () -> assertThrows(Throwable.class, () -> deserialize("00")),
        // from HashCode::fromString
        () -> assertThrows(Throwable.class, () -> deserialize("000")));
  }

  @Test
  @SneakyThrows
  void convertToCursor_returns_no_cursor_if_cant_serialize() {
    var plan = new TestOperator(42);
    plan.setThrowNoCursorOnWrite(true);
    assertAll(
        () -> assertThrows(NoCursorException.class, () -> serialize(plan)),
        () -> assertEquals(Cursor.None, planCache.convertToCursor(plan)));
  }

  @Test
  @SneakyThrows
  void convertToCursor_returns_no_cursor_if_plan_is_not_paginate() {
    var plan = mock(PhysicalPlan.class);
    assertEquals(Cursor.None, planCache.convertToCursor(plan));
  }

  @Test
  void convertToPlan_throws_cursor_has_no_prefix() {
    assertThrows(UnsupportedOperationException.class, () -> planCache.convertToPlan("abc"));
  }

  @Test
  void convertToPlan_throws_if_failed_to_deserialize() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> planCache.convertToPlan("n:" + serialize(mock(Serializable.class))));
  }

  @Test
  @SneakyThrows
  void serialize_and_deserialize() {
    var plan = new TestOperator(42);
    var roundTripPlan = planCache.deserialize(planCache.serialize(plan));
    assertEquals(roundTripPlan, plan);
    assertNotSame(roundTripPlan, plan);
  }

  @Test
  void convertToCursor_and_convertToPlan() {
    var plan = new TestOperator(100500);
    var roundTripPlan =
        (SerializablePlan) planCache.convertToPlan(planCache.convertToCursor(plan).toString());
    assertEquals(plan, roundTripPlan);
    assertNotSame(plan, roundTripPlan);
  }

  @Test
  @SneakyThrows
  void resolveObject() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject("Hello, world!");
    objectOutput.flush();

    var cds =
        planCache.getCursorDeserializationStream(new ByteArrayInputStream(output.toByteArray()));
    assertEquals(storageEngine, cds.resolveObject("engine"));
    var object = new Object();
    assertSame(object, cds.resolveObject(object));
  }

  // Helpers and auxiliary classes section below

  @SneakyThrows
  private String serialize(Serializable input) {
    return new PlanSerializer(null).serialize(input);
  }

  private Serializable deserialize(String input) {
    return new PlanSerializer(null).deserialize(input);
  }
}
