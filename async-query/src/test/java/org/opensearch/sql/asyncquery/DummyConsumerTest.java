/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DummyConsumerTest {

  @Mock Dummy dummy;

  @Test
  public void test() {
    DummyConsumer dummyConsumer = new DummyConsumer(dummy);
    when(dummy.hello()).thenReturn("Hello from mock");

    assertEquals("Hello from mock", dummyConsumer.hello());
  }
}
