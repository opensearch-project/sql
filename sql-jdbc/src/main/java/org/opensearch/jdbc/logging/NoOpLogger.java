/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

/**
 * An No-Op logger that indicates to a caller that all log levels are
 * disabled and does nothing for any other logging method invocations.
 *
 * Classes that use a Logger would typically need to do a null
 * check on the handle if the handle were to be null when logging is
 * disabled. To avoid having such checks on every use of a logger,
 * this no-op logger can be set as the logger when logging is disabled.
 *
 * The consumer of this logger sees all log levels as disabled and any
 * inadvertent calls to log messages are a no-op.
 */
public class NoOpLogger implements Logger {

    public static final NoOpLogger INSTANCE = new NoOpLogger();

    private NoOpLogger() {
        // singleton
    }

    @Override
    public LogLevel getLevel() {
        return LogLevel.OFF;
    }

    @Override
    public void debug(String message) {

    }

    @Override
    public void debug(String message, Throwable t) {

    }

    @Override
    public void error(String message) {

    }

    @Override
    public void error(String message, Throwable t) {

    }

    @Override
    public void fatal(String message) {

    }

    @Override
    public void fatal(String message, Throwable t) {

    }

    @Override
    public void info(String message) {

    }

    @Override
    public void info(String message, Throwable t) {

    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isFatalEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void trace(String message) {

    }

    @Override
    public void trace(String message, Throwable t) {

    }

    @Override
    public void warn(String message) {

    }

    @Override
    public void warn(String message, Throwable t) {

    }

    @Override
    public void close() {

    }
}
