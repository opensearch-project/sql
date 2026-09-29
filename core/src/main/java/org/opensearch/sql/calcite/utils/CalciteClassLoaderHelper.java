/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.concurrent.Callable;

/**
 * Helper for setting the thread context classloader before Calcite operations. This is needed for
 * patched Calcite (CALCITE-3745): when analytics-engine is the parent classloader, Janino uses the
 * parent's classloader which can't see SQL plugin classes. The patched Calcite checks {@code
 * Thread.currentThread().getContextClassLoader()} first. This helper sets it to the SQL plugin's
 * classloader (child) which can see both parent and child classes.
 *
 * @see <a href="https://issues.apache.org/jira/browse/CALCITE-3745">CALCITE-3745</a>
 * @see <a href="https://github.com/opensearch-project/sql/issues/5306">sql#5306</a>
 */
public final class CalciteClassLoaderHelper {

  private CalciteClassLoaderHelper() {}

  /**
   * Run an action with the thread context classloader set to the caller's classloader.
   *
   * @param action the action to run
   * @param callerClass the class whose classloader should be used (pass {@code MyClass.class})
   * @param <T> the return type
   * @return the result of the action
   */
  public static <T> T withCalciteClassLoader(Callable<T> action, Class<?> callerClass) {
    Thread currentThread = Thread.currentThread();
    ClassLoader originalCl = currentThread.getContextClassLoader();
    currentThread.setContextClassLoader(callerClass.getClassLoader());
    try {
      return action.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      currentThread.setContextClassLoader(originalCl);
    }
  }

  /**
   * Run a void action with the thread context classloader set to the caller's classloader.
   *
   * @see #withCalciteClassLoader(Callable, Class)
   */
  public static void withCalciteClassLoader(Runnable action, Class<?> callerClass) {
    withCalciteClassLoader(
        () -> {
          action.run();
          return null;
        },
        callerClass);
  }
}
