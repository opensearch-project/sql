package org.opensearch.sql.spark.execution.session;

public interface SessionConfigSupplier {

  Long getSessionInactivityTimeoutMillis();
}
