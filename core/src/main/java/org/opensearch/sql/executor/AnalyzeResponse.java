/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import org.opensearch.sql.monitor.profile.QueryProfile;

@Data
@Builder
public class AnalyzeResponse {
  private final List<String> logicalPlan;
  private final List<String> physicalPlan;
  private final QueryProfile profile;
  private final List<Recommendation> recommendations;
  private final List<SchemaColumn> schema;
  private final Object[][] datarows;
  private final long total;
  private final long size;

  @Data
  @Builder
  public static class SchemaColumn {
    private final String name;
    private final String type;
  }

  public enum RecommendationSeverityLevel {
    INFO,
    WARNING,
    CRITICAL
  }

  @Data
  @Builder
  public static class Recommendation {
    private final RecommendationSeverityLevel severity;
    private final String rule;
    private final String message;
    private final String affected_node;
    private final String suggestion;
  }
}
