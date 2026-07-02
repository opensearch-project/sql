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

  private final String query;
  private final List<QuerySegment> querySegments;
  // private final String ast;
  private final List<String> logicalPlan;
  private final List<String> physicalPlan;
  private final QueryProfile profile;
  private final List<OperatorNode> operator_tree;
  private final List<String> recommendations;
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

  @Data
  @Builder
  public static class QuerySegment {
    private final String nodeType;
    private final String source;
  }

  @Data
  @Builder
  public static class OperatorNode {
    private final String source;
    private final List<String> node_type;
    private final List<String> description;
    private final String estimated_cost;
    private final Long estimated_rows;
    private final String actual_time_ms;
    private final Long actual_rows;
    private final Boolean is_pushed_down;
  }
}
