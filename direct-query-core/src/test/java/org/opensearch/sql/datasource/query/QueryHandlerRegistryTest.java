/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.client.DataSourceClient;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class QueryHandlerRegistryTest {

  @Mock private QueryHandler<TestClient> mockHandler1;
  @Mock private QueryHandler<AnotherTestClient> mockHandler2;

  @Test
  void should_return_empty_when_registry_is_empty() {
    QueryHandlerRegistry registry = new QueryHandlerRegistry(Collections.emptyList());
    Optional<QueryHandler<TestClient>> result = registry.getQueryHandler(new TestClient());
    assertFalse(result.isPresent());
  }

  @Test
  void should_return_handler_when_client_matches() {
    // Setup
    when(mockHandler1.getClientClass()).thenReturn(TestClient.class);
    when(mockHandler1.canHandle(any(TestClient.class))).thenReturn(true);
    QueryHandlerRegistry registry =
        new QueryHandlerRegistry(Collections.singletonList(mockHandler1));

    // Execute
    Optional<QueryHandler<TestClient>> result = registry.getQueryHandler(new TestClient());

    // Verify
    assertTrue(result.isPresent());
    assertEquals(mockHandler1, result.get());
  }

  @Test
  void should_return_empty_when_handler_cannot_handle_client() {
    // Setup
    when(mockHandler1.getClientClass()).thenReturn(TestClient.class);
    when(mockHandler1.canHandle(any(TestClient.class))).thenReturn(false);
    QueryHandlerRegistry registry =
        new QueryHandlerRegistry(Collections.singletonList(mockHandler1));

    // Execute
    Optional<QueryHandler<TestClient>> result = registry.getQueryHandler(new TestClient());

    // Verify
    assertFalse(result.isPresent());
  }

  @Test
  void should_return_correct_handler_when_multiple_handlers_exist() {
    // Setup
    when(mockHandler1.getClientClass()).thenReturn(TestClient.class);
    when(mockHandler2.getClientClass()).thenReturn(AnotherTestClient.class);
    when(mockHandler1.canHandle(any(TestClient.class))).thenReturn(true);
    when(mockHandler2.canHandle(any(AnotherTestClient.class))).thenReturn(true);
    QueryHandlerRegistry registry =
        new QueryHandlerRegistry(Arrays.asList(mockHandler1, mockHandler2));

    // Execute
    Optional<QueryHandler<TestClient>> result1 = registry.getQueryHandler(new TestClient());
    Optional<QueryHandler<AnotherTestClient>> result2 =
        registry.getQueryHandler(new AnotherTestClient());

    // Verify
    assertTrue(result1.isPresent());
    assertEquals(mockHandler1, result1.get());

    assertTrue(result2.isPresent());
    assertEquals(mockHandler2, result2.get());
  }

  @Test
  void should_handle_class_cast_exception_gracefully() {
    // Setup - create a handler with incompatible client class
    @SuppressWarnings("unchecked")
    QueryHandler<IncompatibleDataSourceClient> badHandler = mock(QueryHandler.class);
    when(badHandler.getClientClass()).thenReturn((Class) IncompatibleDataSourceClient.class);

    // Setup regular handler
    when(mockHandler1.getClientClass()).thenReturn(TestClient.class);
    when(mockHandler1.canHandle(any(TestClient.class))).thenReturn(true);

    // Create registry with both handlers
    QueryHandlerRegistry registry =
        new QueryHandlerRegistry(Arrays.asList(badHandler, mockHandler1));

    // Execute
    Optional<QueryHandler<TestClient>> result = registry.getQueryHandler(new TestClient());

    // Verify - should skip the incompatible handler and find the compatible one
    assertTrue(result.isPresent());
    assertEquals(mockHandler1, result.get());
  }

  @Test
  void should_catch_class_cast_exception_during_can_handle_check() {
    // Setup - create a handler that throws ClassCastException when canHandle is called
    @SuppressWarnings("unchecked")
    QueryHandler<TestClient> problematicHandler = mock(QueryHandler.class);
    when(problematicHandler.getClientClass()).thenReturn(TestClient.class);
    when(problematicHandler.canHandle(any(TestClient.class))).thenThrow(ClassCastException.class);

    // Setup regular handler
    when(mockHandler1.getClientClass()).thenReturn(TestClient.class);
    when(mockHandler1.canHandle(any(TestClient.class))).thenReturn(true);

    // Create registry with both handlers
    QueryHandlerRegistry registry =
        new QueryHandlerRegistry(Arrays.asList(problematicHandler, mockHandler1));

    // Execute
    Optional<QueryHandler<TestClient>> result = registry.getQueryHandler(new TestClient());

    // Verify - should skip the handler that throws exception and find the working one
    assertTrue(result.isPresent());
    assertEquals(mockHandler1, result.get());
  }

  // Test client classes
  private static class TestClient implements DataSourceClient {}

  private static class AnotherTestClient implements DataSourceClient {}

  private static class IncompatibleDataSourceClient implements DataSourceClient {}
}
