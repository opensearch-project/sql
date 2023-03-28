/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class TableTest {

  @Test
  public void createPagedScanBuilder_throws() {
    var table = mock(Table.class, withSettings().defaultAnswer(InvocationOnMock::callRealMethod));
    assertThrows(Throwable.class, () -> table.createPagedScanBuilder(0));
  }
}
