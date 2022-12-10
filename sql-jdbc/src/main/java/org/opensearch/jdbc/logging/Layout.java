/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

public interface Layout {
    String formatLogEntry(LogLevel severity, String message);
}
