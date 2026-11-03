/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ThreadResourceAccountingTest {

  @Test
  void enterOpensTheTasksOwnScope() {
    AtomicInteger closed = new AtomicInteger();
    ThreadResourceAccounting.Scope scope = closed::incrementAndGet;
    ThreadResourceAccounting task = () -> scope;

    ThreadResourceAccounting.Scope entered = ThreadResourceAccounting.enter(task);

    assertSame(scope, entered);
    entered.close();
    assertEquals(1, closed.get());
  }

  @Test
  void enterIsANoOpForATaskThatDoesNoAccounting() {
    assertSame(ThreadResourceAccounting.Scope.NOOP, ThreadResourceAccounting.enter(new Object()));
    assertSame(ThreadResourceAccounting.Scope.NOOP, ThreadResourceAccounting.enter(null));
    assertDoesNotThrow(ThreadResourceAccounting.Scope.NOOP::close);
  }
}
