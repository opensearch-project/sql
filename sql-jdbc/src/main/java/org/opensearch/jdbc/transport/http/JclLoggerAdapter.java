/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport.http;

import org.opensearch.jdbc.logging.Logger;
import org.opensearch.jdbc.logging.LoggingSource;
import org.apache.commons.logging.Log;

public class JclLoggerAdapter implements Log, LoggingSource {
    private final Logger logger;
    private String source;

    public JclLoggerAdapter(Logger logger, String source) {
        this.logger = logger;
        this.source = source;
    }

    @Override
    public void debug(Object message) {
        logger.debug(() -> logMessage(String.valueOf(message)));
    }

    @Override
    public void debug(Object message, Throwable t) {
        logger.debug(String.valueOf(message), t);
    }

    @Override
    public void error(Object message) {
        logger.error(String.valueOf(message));
    }

    @Override
    public void error(Object message, Throwable t) {
        logger.error(String.valueOf(message), t);
    }

    @Override
    public void fatal(Object message) {
        logger.fatal(String.valueOf(message));
    }

    @Override
    public void fatal(Object message, Throwable t) {
        logger.fatal(String.valueOf(message), t);
    }

    @Override
    public void info(Object message) {
        logger.info(String.valueOf(message));
    }

    @Override
    public void info(Object message, Throwable t) {
        logger.info(String.valueOf(message), t);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isFatalEnabled() {
        return logger.isFatalEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void trace(Object message) {
        logger.trace(() -> logMessage(String.valueOf(message)));
    }

    @Override
    public void trace(Object message, Throwable t) {
        logger.trace(String.valueOf(message), t);
    }

    @Override
    public void warn(Object message) {
        logger.warn(String.valueOf(message));
    }

    @Override
    public void warn(Object message, Throwable t) {
        logger.warn(String.valueOf(message), t);
    }

    @Override
    public String getSource() {
        return source;
    }
}
