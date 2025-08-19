/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RetryProcessor extends TestWatcher {

  @Override
  public Statement apply(Statement base, Description description) {
    Retry retry = description.getAnnotation(Retry.class);
    if (retry == null) {
      return base;
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Throwable lastException = null;
        for (int i = 0; i < retry.value(); i++) {
          try {
            base.evaluate();
            return;
          } catch (Throwable t) {
            lastException = t;
            System.out.println(
                "Retrying " + description.getDisplayName() + " " + (i + 1) + " times");
          }
        }
        assert lastException != null;
        throw lastException;
      }
    };
  }
}
