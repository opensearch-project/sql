/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration tests for VPC PPL dashboard queries. These tests ensure that VPC-related PPL queries
 * work correctly with actual test data.
 */
public class VpcPplDashboardIT extends PPLIntegTestCase {

  private static final String VPC_LOGS_INDEX = "vpc_logs";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadVpcLogsIndex();
  }

  private void loadVpcLogsIndex() throws IOException {
    if (!TestUtils.isIndexExist(client(), VPC_LOGS_INDEX)) {
      String mapping = TestUtils.getMappingFile("vpc_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), VPC_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(client(), VPC_LOGS_INDEX, "src/test/resources/vpc_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count()", VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(3)); // Actual count from test data
  }

  @Test
  public void testTotalFlowsByActions() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.vpc.action` | head 5", VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("aws.vpc.action", null, "string"));
    // Should have ACCEPT and REJECT actions from test data
    verifyDataRows(response, rows(2, "ACCEPT"), rows(1, "REJECT"));
  }

  @Test
  public void testRequestHistory() throws IOException {
    String query = String.format("source=%s | stats count() by `@timestamp`", VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("count()", null, "bigint"), schema("@timestamp", null, "timestamp"));
    // Each record has unique timestamp, so 3 groups
    verifyDataRows(
        response,
        rows(1, "2024-01-15 10:00:00"),
        rows(1, "2024-01-15 10:00:05"),
        rows(1, "2024-01-15 10:00:10"));
  }

  @Test
  public void testRequestsByDirection() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as flow_direction by `aws.vpc.flow-direction` | sort -"
                + " flow_direction | head 5",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("flow_direction", null, "bigint"),
        schema("aws.vpc.flow-direction", null, "string"));
    // Should have ingress and egress flow directions
    verifyDataRows(response, rows(2, "ingress"), rows(1, "egress"));
  }

  @Test
  public void testBytes() throws IOException {
    String query =
        String.format("source=%s | stats sum(`aws.vpc.bytes`) by `@timestamp`", VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(`aws.vpc.bytes`)", null, "bigint"),
        schema("@timestamp", null, "timestamp"));
    // Each record has unique timestamp, so 3 groups
    verifyDataRows(
        response,
        rows(1500, "2024-01-15 10:00:00"),
        rows(750, "2024-01-15 10:00:05"),
        rows(3000, "2024-01-15 10:00:10"));
  }

  @Test
  public void testPackets() throws IOException {
    String query =
        String.format("source=%s | stats sum(`aws.vpc.packets`) by `@timestamp`", VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(`aws.vpc.packets`)", null, "bigint"),
        schema("@timestamp", null, "timestamp"));
    // Each record has unique timestamp, so 3 groups
    verifyDataRows(
        response,
        rows(10, "2024-01-15 10:00:00"),
        rows(5, "2024-01-15 10:00:05"),
        rows(20, "2024-01-15 10:00:10"));
  }

  @Test
  public void testTopSourceAwsServices() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as `src-aws-service` by `aws.vpc.pkt-src-aws-service` | sort"
                + " - `src-aws-service` | head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("src-aws-service", null, "bigint"),
        schema("aws.vpc.pkt-src-aws-service", null, "string"));
    verifyDataRows(response, rows(2, "AMAZON"), rows(1, "EC2"));
  }

  @Test
  public void testTopDestinationAwsServices() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as `dst-aws-service` by `aws.vpc.pkt-dst-aws-service` | sort"
                + " - `dst-aws-service` | head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("dst-aws-service", null, "bigint"),
        schema("aws.vpc.pkt-dst-aws-service", null, "string"));
    verifyDataRows(response, rows(2, "EC2"), rows(1, "AMAZON"));
  }

  @Test
  public void testRequestsByDirectionMetric() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Requests by `aws.vpc.flow-direction` | sort - Requests |"
                + " head 5",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Requests", null, "bigint"),
        schema("aws.vpc.flow-direction", null, "string"));
    verifyDataRows(response, rows(2, "ingress"), rows(1, "egress"));
  }

  @Test
  public void testTopDestinationBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`aws.vpc.bytes`) as Bytes by `aws.vpc.dstaddr` | sort - Bytes |"
                + " head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Bytes", null, "bigint"), schema("aws.vpc.dstaddr", null, "string"));
    verifyDataRows(
        response, rows(3000, "192.168.2.100"), rows(1500, "10.0.2.200"), rows(750, "10.0.1.100"));
  }

  @Test
  public void testTopSourceBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`aws.vpc.bytes`) as Bytes by `aws.vpc.srcaddr` | sort - Bytes |"
                + " head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Bytes", null, "bigint"), schema("aws.vpc.srcaddr", null, "string"));
    verifyDataRows(
        response, rows(3000, "192.168.1.50"), rows(1500, "10.0.1.100"), rows(750, "10.0.2.200"));
  }

  @Test
  public void testTopSources() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Requests by `aws.vpc.srcaddr` | sort - Requests | head"
                + " 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Requests", null, "bigint"), schema("aws.vpc.srcaddr", null, "string"));
    verifyDataRows(response, rows(1, "10.0.1.100"), rows(1, "10.0.2.200"), rows(1, "192.168.1.50"));
  }

  @Test
  public void testTopDestinations() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Requests by `aws.vpc.dstaddr` | sort - Requests | head"
                + " 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Requests", null, "bigint"), schema("aws.vpc.dstaddr", null, "string"));
    verifyDataRows(
        response, rows(1, "10.0.2.200"), rows(1, "10.0.1.100"), rows(1, "192.168.2.100"));
  }

  @Test
  public void testHeatMap() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.vpc.dstaddr`, `aws.vpc.srcaddr` | sort -"
                + " Count | head 20",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.vpc.dstaddr", null, "string"),
        schema("aws.vpc.srcaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1, "10.0.2.200", "10.0.1.100"),
        rows(1, "10.0.1.100", "10.0.2.200"),
        rows(1, "192.168.2.100", "192.168.1.50"));
  }

  @Test
  public void testVpcLiveRawSearch() throws IOException {
    String query =
        String.format(
            "source=%s | fields `@timestamp`, `start_time`, `interval_start_time`, `end_time`,"
                + " `aws.vpc.srcport`, `aws.vpc.pkt-src-aws-service`, `aws.vpc.srcaddr`,"
                + " `aws.vpc.src-interface_uid`, `aws.vpc.src-vpc_uid`, `aws.vpc.src-instance_uid`,"
                + " `aws.vpc.src-subnet_uid`, `aws.vpc.dstport`, `aws.vpc.pkt-dst-aws-service`,"
                + " `aws.vpc.dstaddr`, `aws.vpc.flow-direction`, `aws.vpc.connection.tcp_flags`,"
                + " `aws.vpc.packets`, `aws.vpc.bytes`, `aws.vpc.status_code`, `aws.vpc.version`,"
                + " `aws.vpc.type_name`, `aws.vpc.traffic_path`, `aws.vpc.az_id`, `aws.vpc.action`,"
                + " `aws.vpc.region`, `aws.vpc.account-id`, `aws.vpc.sublocation_type`,"
                + " `aws.vpc.sublocation_id` | sort - `@timestamp`",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    assertEquals(3, response.getJSONArray("datarows").length());
  }

  @Test
  public void testFlow() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.vpc.dstaddr`, `aws.vpc.srcaddr` | head"
                + " 10000",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.vpc.dstaddr", null, "string"),
        schema("aws.vpc.srcaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1, "10.0.2.200", "10.0.1.100"),
        rows(1, "10.0.1.100", "10.0.2.200"),
        rows(1, "192.168.2.100", "192.168.1.50"));
  }

  @Test
  public void testTopTalkersByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`aws.vpc.packets`) as Packets by `aws.vpc.srcaddr` | sort -"
                + " Packets | head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Packets", null, "bigint"), schema("aws.vpc.srcaddr", null, "string"));
    verifyDataRows(
        response, rows(20, "192.168.1.50"), rows(10, "10.0.1.100"), rows(5, "10.0.2.200"));
  }

  @Test
  public void testTopDestinationsByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`aws.vpc.packets`) as Packets by `aws.vpc.dstaddr` | sort -"
                + " Packets | head 10",
            VPC_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Packets", null, "bigint"), schema("aws.vpc.dstaddr", null, "string"));
    verifyDataRows(
        response, rows(20, "192.168.2.100"), rows(10, "10.0.2.200"), rows(5, "10.0.1.100"));
  }
}
