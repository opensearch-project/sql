package org.opensearch.sql.expression.datetime;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.opensearch.sql.planner.physical.SessionContext;

class TestSessionContext implements SessionContext {
  Clock startClock;

  public TestSessionContext() {
    startClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
  }

  @Override
  public Clock getQueryStartClock() {
    return startClock;
  }

  @Override
  public Clock getSystemClock() {
    return Clock.systemDefaultZone();
  }
}
