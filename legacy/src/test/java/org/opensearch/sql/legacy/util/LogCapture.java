/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;

/** Captures the events a logger writes while an action runs, at every level down to DEBUG. */
public final class LogCapture {

  /** An action that may throw. */
  @FunctionalInterface
  public interface Action {
    void run() throws Exception;
  }

  private LogCapture() {}

  /**
   * Runs the action and returns the events written to the logger of the given class.
   *
   * @param loggerClass the class whose logger is captured
   * @param action the action to run
   * @return the captured events in the order they were written
   */
  public static List<LogEvent> capture(Class<?> loggerClass, Action action) throws Exception {
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(loggerClass);
    Level originalLevel = logger.getLevel();
    Configurator.setLevel(logger.getName(), Level.DEBUG);
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
      Configurator.setLevel(logger.getName(), originalLevel);
    }
    return events;
  }
}
