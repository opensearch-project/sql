/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PushDownRequestBuilderTest {

  @Test
  public void throw_unsupported2() {
    var builder = mock(PushDownRequestBuilder.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));

    assertAll(
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownFilter(null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownAggregation(null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownSort(null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownLimit(null, null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownHighlight(null, null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushDownProjects(null)),
        () -> assertThrows(UnsupportedOperationException.class, () ->
            builder.pushTypeMapping(null))
    );
  }
}
