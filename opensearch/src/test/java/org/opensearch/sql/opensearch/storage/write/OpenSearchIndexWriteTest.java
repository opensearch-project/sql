/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexWriteTest {

  @Mock
  private PhysicalPlan input;

  @Mock
  private OpenSearchClient client;

  @Mock
  private Settings settings;

  private final IndexName indexName = new IndexName("test");

  private final List<String> columns = Arrays.asList("name", "age");

  private OpenSearchIndexWrite indexWrite;

  @BeforeEach
  void setUp() {
    indexWrite = new OpenSearchIndexWrite(input, client, settings, indexName, columns);
  }

  @Test
  void testSchema() {
    assertEquals(
        new ExecutionEngine.Schema(
            Collections.singletonList(new Column("message", "message", STRING))),
        indexWrite.schema());
  }

  @Test
  void testLoadValues() {
    when(input.hasNext()).thenReturn(true, false);
    when(input.next()).thenReturn(collectionValue(Arrays.asList("John", 25)));

    assertThat(
        execute(),
        allOf(
            iterableWithSize(1),
            hasItems(
                tupleValue(ImmutableMap.of("message", "1 row(s) impacted")))));
    verify(client, times(1)).bulk("test", List.of(Map.of("name", "John", "age", 25)));
  }

  @Test
  void testLoadTuples() {
    when(input.hasNext()).thenReturn(true, false);
    when(input.next()).thenReturn(tupleValue(Map.of("name", "John", "age", 25)));

    assertThat(
        execute(),
        allOf(
            iterableWithSize(1),
            hasItems(
                tupleValue(ImmutableMap.of("message", "1 row(s) impacted")))));
    verify(client, times(1)).bulk("test", List.of(Map.of("name", "John", "age", 25)));
  }

  private List<ExprValue> execute() {
    List<ExprValue> results = new ArrayList<>();
    indexWrite.open();
    while (indexWrite.hasNext()) {
      results.add(indexWrite.next());
    }
    indexWrite.close();
    return results;
  }
}