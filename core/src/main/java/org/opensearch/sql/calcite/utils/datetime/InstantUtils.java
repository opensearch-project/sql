package org.opensearch.sql.calcite.utils.datetime;

import java.time.*;
import org.apache.calcite.sql.type.SqlTypeName;

public final class InstantUtils {
    private InstantUtils() {}

    /**
     * Convert epoch milliseconds to Instant.
     *
     * @param epochMillis epoch milliseconds
     * @return Instant that represents the given epoch milliseconds
     */
    public static Instant fromEpochMills(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis);
    }

    /**
     * Convert internal date to Instant.
     *
     * @param date internal date in days since epoch
     * @return Instant that represents the given date at timezone UTC at 00:00:00
     */
    public static Instant fromInternalDate(int date) {
        LocalDate localDate = LocalDate.ofEpochDay(date);
        return localDate.atStartOfDay(ZoneId.of("UTC")).toInstant();
    }

    /**
     * Convert internal time to Instant.
     *
     * @param time internal time in milliseconds
     * @return Instant that represents the current day with the given time at timezone UTC
     */
    public static Instant fromInternalTime(int time) {
        LocalDate todayUtc = LocalDate.now(ZoneId.of("UTC"));
        ZonedDateTime startOfDayUtc = todayUtc.atStartOfDay(ZoneId.of("UTC"));
        return startOfDayUtc.toInstant().plus(Duration.ofMillis(time));
    }

    /**
     * Convert internal date/time/timestamp to Instant.
     *
     * @param internalDatetime internal date/time/timestamp. Date is represented as days since epoch,
     *     time is represented as milliseconds, and timestamp is represented as epoch milliseconds
     * @param type type of the internalDatetime
     * @return Instant that represents the given internalDatetime
     */
    public static Instant convertToInstant(Number internalDatetime, SqlTypeName type) {
        return switch (type) {
            case TIME -> InstantUtils.fromInternalTime(internalDatetime.intValue());
            case TIMESTAMP -> InstantUtils.fromEpochMills(internalDatetime.longValue());
            case DATE -> InstantUtils.fromInternalDate(internalDatetime.intValue());
            default -> throw new IllegalArgumentException(
                    "Invalid argument type. Must be TIME, but got " + type);
        };
    }
}

