package org.opensearch.sql.prometheus.storage.script.filter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class PromFilterQuery {
  private StringBuilder promQl;
  private Long startTime;
  private Long endTime;
  private String step;
}
