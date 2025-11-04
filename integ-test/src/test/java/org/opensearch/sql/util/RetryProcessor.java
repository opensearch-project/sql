/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/** Retry processor to retry a test when exception happens. Retry a test by adding @Retry. */
public class RetryProcessor extends TestWatcher {
  private static final Logger LOG = LogManager.getLogger();

  @Override
  public Statement apply(Statement base, Description description) {
    Retry retry =
        Optional.ofNullable(description.getAnnotation(Retry.class))
            .orElseGet(() -> description.getTestClass().getAnnotation(Retry.class));
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
            LOG.info("Retrying {} {} times", description.getDisplayName(), (i + 1));
            Thread.sleep(3000);
          }
        }
        assert lastException != null;
        throw lastException;
      }
    };
  }
}
