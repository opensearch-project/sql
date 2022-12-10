/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

/**
 * Enumeration of possible Logging levels
 */
public enum LogLevel {

    /**
     * Log nothing
     */
    OFF(0),

    /**
     * Log only fatal errors
     */
    FATAL(10),

    /**
     * Log all errors
     */
    ERROR(20),

    /**
     * Log all errors and warnings
     */
    WARN(30),

    /**
     * Log all errors, warnings and info messages
     */
    INFO(40),

    /**
     * Log everything up to INFO and any debug logs
     */
    DEBUG(50),

    /**
     * Log everything up to DEBUG and any additional fine grained
     * trace logs
     */
    TRACE(60),

    /**
     * Log everything
     */
    ALL(100);

    /**
     * Internal severity level indicator for the log level
     */
    private int severity;

    LogLevel(final int severity) {
        this.severity = severity;
    }

    /**
     * @return internal severity level associated with the log level
     */
    private int severity() {
        return severity;
    }

    /**
     * Checks if this LogLevel corresponds to a severity level
     * matching or exceeding the severity level of a specified LogLevel.
     *
     * @param level the logging level to compare this log level with
     *
     * @return true, if the severity of this log level matches or
     *         exceeds the severity of the specified log level, false otherwise.
     */
    public boolean isGreaterThanOrEqualTo(LogLevel level) {
        return this.severity() >= level.severity();
    }
}
