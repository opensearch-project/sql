/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer.FlattenResult;
import org.opensearch.sql.executor.pagination.serde.IndexScanNode;
import org.opensearch.sql.executor.pagination.serde.ProjectNode;
import org.opensearch.sql.executor.pagination.serde.SerializablePlanNode;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.utils.DeserializationFilterUtil;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PlanSerializerTest {

  private StorageEngine storageEngine;
  private ObjectMapper writeMapper;
  private PlanSerializer.PlanFlattenFunction flattener;
  private PlanSerializer.PlanRebuildFunction rebuilder;
  private PlanSerializer planSerializer;

  @BeforeEach
  void setUp() {
    storageEngine = mock(StorageEngine.class);
    writeMapper = new ObjectMapper(new SmileFactory());
    flattener = mock(PlanSerializer.PlanFlattenFunction.class);
    rebuilder = mock(PlanSerializer.PlanRebuildFunction.class);
    planSerializer = new PlanSerializer(storageEngine, null, writeMapper, flattener, rebuilder);
  }

  @Test
  void convertToCursor_returns_no_cursor_if_plan_is_not_supported() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    when(flattener.flatten(eq(plan), nullable(Settings.class))).thenThrow(new NoCursorException());

    assertEquals(Cursor.None, planSerializer.convertToCursor(plan));
  }

  @Test
  void successful_cursor_uses_smile_and_commits_ownership() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    SerializablePlanNode node = new IndexScanNode(new byte[] {1, 2, 3}, 10);
    AtomicBoolean committed = new AtomicBoolean();
    when(flattener.flatten(eq(plan), nullable(Settings.class)))
        .thenReturn(new FlattenResult(node, () -> committed.set(true)));

    Cursor cursor = planSerializer.convertToCursor(plan);

    assertTrue(committed.get());
    assertTrue(cursor.toString().startsWith(PlanSerializer.CURSOR_PREFIX));
    String encoded = cursor.toString().substring(PlanSerializer.CURSOR_PREFIX.length());
    byte[] smileBytes = Base64.getDecoder().decode(encoded);
    assertEquals(SmileConstants.HEADER_BYTE_1, smileBytes[0]);
    assertEquals(SmileConstants.HEADER_BYTE_2, smileBytes[1]);
    assertEquals(SmileConstants.HEADER_BYTE_3, smileBytes[2]);
  }

  @Test
  void encoding_failure_does_not_commit_ownership() throws Exception {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    SerializablePlanNode node = new IndexScanNode(new byte[] {1}, 10);
    AtomicBoolean committed = new AtomicBoolean();
    when(flattener.flatten(eq(plan), nullable(Settings.class)))
        .thenReturn(new FlattenResult(node, () -> committed.set(true)));
    ObjectMapper failingMapper = mock(ObjectMapper.class);
    when(failingMapper.writeValueAsBytes(node))
        .thenThrow(
            new JsonProcessingException("expected encoding failure") {
              private static final long serialVersionUID = 1L;
            });
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, null, failingMapper, flattener, rebuilder);

    assertThrows(IllegalStateException.class, () -> serializer.convertToCursor(plan));
    assertFalse(committed.get());
  }

  @Test
  void valid_smile_cursor_rebuilds_plan() throws Exception {
    SerializablePlanNode node = new IndexScanNode(new byte[] {1, 2, 3}, 10);
    PhysicalPlan expected = mock(PhysicalPlan.class);
    when(rebuilder.rebuild(any(SerializablePlanNode.class), any(StorageEngine.class)))
        .thenReturn(expected);

    assertSame(expected, planSerializer.convertToPlan(cursorFor(node)));
  }

  @Test
  void payload_exceeding_write_limit_does_not_commit_ownership() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    SerializablePlanNode node = new IndexScanNode(new byte[256], 10);
    AtomicBoolean committed = new AtomicBoolean();
    when(flattener.flatten(eq(plan), nullable(Settings.class)))
        .thenReturn(new FlattenResult(node, () -> committed.set(true)));
    Settings settings = settingsWith(100, 1000, 32);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    assertThrows(IllegalArgumentException.class, () -> serializer.convertToCursor(plan));
    assertFalse(committed.get());
  }

  @Test
  void cursor_byte_limit_is_independent_from_java_expression_stream_limit() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    SerializablePlanNode node = new IndexScanNode(new byte[256], 10);
    AtomicBoolean committed = new AtomicBoolean();
    when(flattener.flatten(eq(plan), nullable(Settings.class)))
        .thenReturn(new FlattenResult(node, () -> committed.set(true)));
    Settings settings = settingsWith(100, 1000, 1, 1024);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    Cursor cursor = serializer.convertToCursor(plan);

    assertTrue(cursor.toString().startsWith(PlanSerializer.CURSOR_PREFIX));
    assertTrue(committed.get());
  }

  @Test
  void cursor_envelope_larger_than_expression_limit_round_trips() throws Exception {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    byte[] requestBytes = new byte[DeserializationFilterUtil.DEFAULT_MAX_BYTES + 1];
    SerializablePlanNode node = new IndexScanNode(requestBytes, 10);
    byte[] smileBytes = writeMapper.writeValueAsBytes(node);
    assertTrue(smileBytes.length > DeserializationFilterUtil.DEFAULT_MAX_BYTES);

    AtomicBoolean committed = new AtomicBoolean();
    when(flattener.flatten(eq(plan), nullable(Settings.class)))
        .thenReturn(new FlattenResult(node, () -> committed.set(true)));
    PhysicalPlan rebuilt = mock(PhysicalPlan.class);
    when(rebuilder.rebuild(any(SerializablePlanNode.class), eq(storageEngine))).thenReturn(rebuilt);
    Settings settings =
        settingsWith(
            100,
            1000,
            DeserializationFilterUtil.DEFAULT_MAX_BYTES,
            PlanSerializer.DEFAULT_MAX_CURSOR_BYTES);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    Cursor cursor = serializer.convertToCursor(plan);
    assertTrue(committed.get());
    assertSame(rebuilt, serializer.convertToPlan(cursor.toString()));
  }

  @Test
  void serde_nodes_defensively_copy_mutable_components() {
    byte[] sourceBytes = new byte[] {1, 2, 3};
    IndexScanNode indexScan = new IndexScanNode(sourceBytes, 10);
    sourceBytes[0] = 9;
    byte[] returnedBytes = indexScan.requestBytes();
    returnedBytes[1] = 9;
    assertArrayEquals(new byte[] {1, 2, 3}, indexScan.requestBytes());

    List<String> sourceExpressions = new ArrayList<>(List.of("name"));
    ProjectNode project = new ProjectNode(sourceExpressions, indexScan);
    sourceExpressions.add("age");
    assertEquals(List.of("name"), project.projectList());
  }

  @Test
  void convertToPlan_throws_if_cursor_has_no_prefix() {
    assertThrows(UnsupportedOperationException.class, () -> planSerializer.convertToPlan("abc"));
  }

  @Test
  void convertToPlan_rejects_legacy_format() {
    assertThrows(
        UnsupportedOperationException.class, () -> planSerializer.convertToPlan("n:deadbeef"));
  }

  @Test
  void convertToPlan_throws_if_base64_is_invalid() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> planSerializer.convertToPlan("n:v2:not-valid-base64!!!"));
  }

  @Test
  void convertToPlan_throws_if_smile_is_invalid() {
    assertThrows(
        UnsupportedOperationException.class, () -> planSerializer.convertToPlan("n:v2:AAAA"));
  }

  @Test
  void convertToPlan_enforces_byte_limit() throws Exception {
    SerializablePlanNode node = new IndexScanNode(new byte[] {1, 2, 3}, 10);
    byte[] smileBytes = writeMapper.writeValueAsBytes(node);
    Settings settings = settingsWith(100, 1000, smileBytes.length - 1);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    assertThrows(
        UnsupportedOperationException.class, () -> serializer.convertToPlan(cursorFor(node)));
  }

  @Test
  void convertToPlan_enforces_depth_limit() throws Exception {
    SerializablePlanNode node =
        new ProjectNode(
            List.of("first"),
            new ProjectNode(List.of("second"), new IndexScanNode(new byte[] {1}, 10)));
    Settings settings = settingsWith(1, 1000, 15000);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    assertThrows(
        UnsupportedOperationException.class, () -> serializer.convertToPlan(cursorFor(node)));
  }

  @Test
  void convertToPlan_enforces_reference_limit() throws Exception {
    SerializablePlanNode node = new IndexScanNode(new byte[] {1}, 10);
    Settings settings = settingsWith(100, 1, 15000);
    PlanSerializer serializer =
        new PlanSerializer(storageEngine, settings, writeMapper, flattener, rebuilder);

    assertThrows(
        UnsupportedOperationException.class, () -> serializer.convertToPlan(cursorFor(node)));
  }

  @Test
  void cursor_prefix_is_v2() {
    assertEquals("n:v2:", PlanSerializer.CURSOR_PREFIX);
  }

  private String cursorFor(SerializablePlanNode node) throws Exception {
    return PlanSerializer.CURSOR_PREFIX
        + Base64.getEncoder().encodeToString(writeMapper.writeValueAsBytes(node));
  }

  private static Settings settingsWith(int depth, int refs, int bytes) {
    return settingsWith(depth, refs, bytes, bytes);
  }

  private static Settings settingsWith(int depth, int refs, int expressionBytes, int cursorBytes) {
    Map<Settings.Key, Object> values =
        Map.of(
            Settings.Key.DESERIALIZATION_MAX_DEPTH, depth,
            Settings.Key.DESERIALIZATION_MAX_REFS, refs,
            Settings.Key.DESERIALIZATION_MAX_BYTES, expressionBytes,
            Settings.Key.CURSOR_MAX_BYTES, cursorBytes);
    return new Settings() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> T getSettingValue(Settings.Key key) {
        return (T) values.get(key);
      }

      @Override
      public List<?> getSettings() {
        return List.of();
      }
    };
  }
}
