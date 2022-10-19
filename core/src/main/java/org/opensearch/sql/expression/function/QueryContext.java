package org.opensearch.sql.expression.function;

import java.time.Clock;

/**
 * Class to capture values that may be necessary to implement SQL functions.
 * An example is query execution start time to implement
 */
public interface QueryContext {
  Clock getQueryStartTime();
}
