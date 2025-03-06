package org.opensearch.sql.calcite.utils.datetime;

import java.time.Duration;
import java.time.Instant;

public class DateTimeApplyUtils {
    public static Instant applyInterval(Instant base, Duration interval, boolean isAdd) {
        return isAdd ? base.plus(interval) : base.minus(interval);
    }

}
