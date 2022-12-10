/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

import java.sql.SQLException;

/**
 * An entity that generates log messages containing an identifier for
 * the source of the log message.
 */
public interface LoggingSource {

    default String logMessage(final String format, final Object... args) {
        return logMessage(String.format(format, args));
    }

    default String logMessage(final String message) {
        return buildMessage(message);
    }

    default String logEntry(final String format, final Object... args) {
        return logMessage(String.format(format, args) +" called");
    }

    default String logExit(final String message, final Object returnValue) {
        return logMessage(message +" returning: "+returnValue);
    }

    default String logExit(final String message) {
        return logMessage(message +" returned");
    }

    default String getSource() {
        return this.getClass().getSimpleName() + "@" + Integer.toHexString(this.hashCode());
    }

    default void logAndThrowSQLException(Logger log, SQLException sqlex) throws SQLException {
        logAndThrowSQLException(log, LogLevel.ERROR, sqlex);
    }

    default void logAndThrowSQLException(Logger log, LogLevel severity, SQLException sqlex) throws SQLException {
        logAndThrowSQLException(log, severity, sqlex.getMessage(), sqlex);
    }

    default void logAndThrowSQLException(Logger log, LogLevel severity, String message, SQLException sqlex) throws SQLException {
        if (log.isLevelEnabled(severity)) {

            String logMessage = buildMessage(message);

            switch (severity) {
                case OFF:
                    break;
                case INFO:
                    log.info(logMessage, sqlex);
                    break;
                case WARN:
                    log.warn(logMessage, sqlex);
                    break;
                case DEBUG:
                    log.debug(logMessage, sqlex);
                    break;
                case ERROR:
                    log.error(logMessage, sqlex);
                    break;
                case FATAL:
                    log.fatal(logMessage, sqlex);
                    break;
                case TRACE:
                    log.trace(logMessage, sqlex);
                    break;
                case ALL:
                    log.error(logMessage, sqlex);
                    break;
            }
        }
        throw sqlex;
    }

    default String buildMessage(final  String message) {
        return "["+ getSource()+"] "+message;
    }
}
