/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.sql.Date;

public enum JdbcDateTimeFormatter {

    JDBC_FORMAT("yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss");

    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timestampFormatter;

    JdbcDateTimeFormatter(String dateFormat, String timestampFormat) {
        this.dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        this.timestampFormatter = DateTimeFormatter.ofPattern(timestampFormat);
    }

    public String format(Date date) {
        return date.toLocalDate().format(dateFormatter);
    }

    public String format(Timestamp date) {
        return date.toLocalDateTime().format(timestampFormatter);
    }
}
