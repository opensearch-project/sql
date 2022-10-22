package org.opensearch.sql.planner.physical;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public interface SessionContext {
  SessionContext None = new SessionContext() {
    private final Clock epochStartClock = Clock.fixed(Instant.EPOCH, ZoneId.of("UTC"));
    @Override
    public Clock getQueryStartClock() {
      return epochStartClock;
    }

    @Override
    public Clock getSystemClock() {
      return epochStartClock;
    }
  };

  Clock getQueryStartClock();
  Clock getSystemClock();
}
