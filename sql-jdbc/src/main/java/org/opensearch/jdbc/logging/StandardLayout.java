/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

/**
 * The default log entry layout for driver emitted logs
 *
 * Formats log entries with [timestamp][severity][thread-name] message
 *
 * Timestamp uses ISO format date and a a 24 hour clock value upto
 * milliseconds: [YYYY-mm-dd HH:MM:SS.mmm]
 */
public class StandardLayout implements Layout {
    public static final StandardLayout INSTANCE = new StandardLayout();

    private StandardLayout() {
        // singleton
    }

    @Override
    public String formatLogEntry(LogLevel severity, String message)  {
        long time = System.currentTimeMillis();
        return String.format("[%tF %tT.%tL][%-5s][Thread-%s]%s",
                time, time, time, severity, Thread.currentThread().getName(), message);
    }
}
