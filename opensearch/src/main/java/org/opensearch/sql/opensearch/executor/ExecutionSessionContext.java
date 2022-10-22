package org.opensearch.sql.opensearch.executor;

import java.time.Clock;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.physical.SessionContext;

@RequiredArgsConstructor
public class ExecutionSessionContext implements SessionContext {
  private final Clock queryStartClock;
  private final Clock systemClock;

  @Override
  public Clock getQueryStartClock() {
    return queryStartClock;
  }

  @Override
  public Clock getSystemClock() { return systemClock; }
}
