/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.dashboard;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration tests for VPC Flow Logs PPL dashboard queries. */
public class VpcFlowLogsPplDashboardIT extends PPLIntegTestCase {

  private static final String VPC_FLOW_LOGS_INDEX = "vpc_flow_logs";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadVpcFlowLogsIndex();
  }

  private void loadVpcFlowLogsIndex() throws IOException {
    if (!TestUtils.isIndexExist(client(), VPC_FLOW_LOGS_INDEX)) {
      String mapping = TestUtils.getMappingFile("vpc_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), VPC_FLOW_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(
          client(), VPC_FLOW_LOGS_INDEX, "src/test/resources/vpc_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count()", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testTotalFlowsByActions() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by action | SORT - Count | HEAD 5",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("action", null, "string"));
    verifyDataRows(response, rows(2, "ACCEPT"), rows(1, "REJECT"));
  }

  @Test
  public void testFlowsOvertime() throws IOException {
    String query =
        String.format("source=%s | STATS count() by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("count()", null, "bigint"), schema("span(`start`,30d)", null, "bigint"));
    verifyDataRows(response, rows(3, 0));
  }

  @Test
  public void testRequestsByDirection() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by `flow-direction` | SORT - Count | HEAD 5",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("flow-direction", null, "string"));
    verifyDataRows(response, rows(2, "egress"), rows(1, "ingress"));
  }

  @Test
  public void testBytesTransferredOverTime() throws IOException {
    String query =
        String.format("source=%s | STATS sum(bytes) by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(bytes)", null, "bigint"),
        schema("span(`start`,30d)", null, "bigint"));
    verifyDataRows(response, rows(2640, 0));
  }

  @Test
  public void testPacketsTransferredOverTime() throws IOException {
    String query =
        String.format("source=%s | STATS sum(packets) by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(packets)", null, "bigint"),
        schema("span(`start`,30d)", null, "bigint"));
    verifyDataRows(response, rows(6, 0));
  }

  @Test
  public void testTopSourceAwsServices() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by `pkt-src-aws-service` | SORT - Count | HEAD 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("pkt-src-aws-service", null, "string"));
    verifyDataRows(response, rows(3, "-"));
  }

  @Test
  public void testTopDestinationAwsServices() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by `pkt-dst-aws-service` | SORT - Count | HEAD 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("pkt-dst-aws-service", null, "string"));
    verifyDataRows(response, rows(1, "S3"), rows(1, "EC2"), rows(1, "AMAZON"));
  }

  @Test
  public void testTopDestinationByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(bytes) as Bytes by dstaddr | sort - Bytes | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Bytes", null, "bigint"), schema("dstaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1320, "162.142.125.179"),
        rows(880, "162.142.125.178"),
        rows(440, "162.142.125.177"));
  }

  @Test
  public void testTopTalkersByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(bytes) as Bytes by srcaddr | sort - Bytes | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Bytes", null, "bigint"), schema("srcaddr", null, "string"));
    verifyDataRows(
        response, rows(1320, "10.0.0.202"), rows(880, "10.0.0.201"), rows(440, "10.0.0.200"));
  }

  @Test
  public void testTopTalkersByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(packets) as Packets by srcaddr | sort - Packets | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Packets", null, "bigint"), schema("srcaddr", null, "string"));
    verifyDataRows(response, rows(3, "10.0.0.202"), rows(2, "10.0.0.201"), rows(1, "10.0.0.200"));
  }

  @Test
  public void testTopDestinationsByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(packets) as Packets by dstaddr | sort - Packets | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Packets", null, "bigint"), schema("dstaddr", null, "string"));
    verifyDataRows(
        response,
        rows(3, "162.142.125.179"),
        rows(2, "162.142.125.178"),
        rows(1, "162.142.125.177"));
  }

  @Test
  public void testTopTalkersByIPs() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by srcaddr | SORT - Count | HEAD 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("srcaddr", null, "string"));
    verifyDataRows(response, rows(1, "10.0.0.202"), rows(1, "10.0.0.201"), rows(1, "10.0.0.200"));
  }

  @Test
  public void testTopDestinationsByIPs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Requests by dstaddr | sort - Requests | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Requests", null, "bigint"), schema("dstaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1, "162.142.125.177"),
        rows(1, "162.142.125.178"),
        rows(1, "162.142.125.179"));
  }

  @Test
  public void testTopTalkersByHeatMap() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by dstaddr, srcaddr | sort - Count | head 100",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("dstaddr", null, "string"),
        schema("srcaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1, "162.142.125.177", "10.0.0.200"),
        rows(1, "162.142.125.178", "10.0.0.201"),
        rows(1, "162.142.125.179", "10.0.0.202"));
  }
}
