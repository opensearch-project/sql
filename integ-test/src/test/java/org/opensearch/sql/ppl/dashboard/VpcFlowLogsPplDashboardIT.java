/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
      String mapping = TestUtils.getMappingFile("doctest/mappings/vpc_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), VPC_FLOW_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(
          client(), VPC_FLOW_LOGS_INDEX, "src/test/resources/doctest/testdata/vpc_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count()", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(100));
  }

  @Test
  public void testTotalFlowsByActions() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by action | SORT - Count | HEAD 5",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("action", null, "string"));
    verifyDataRows(response, rows(92, "ACCEPT"), rows(8, "REJECT"));
  }

  @Test
  public void testFlowsOvertime() throws IOException {
    String query =
        String.format("source=%s | STATS count() by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("count()", null, "bigint"),
        schema("span(`start`,30d)", null, "timestamp"));
    verifyDataRows(
        response,
        rows(6, "2025-04-12 00:00:00"),
        rows(24, "2025-05-12 00:00:00"),
        rows(17, "2025-06-11 00:00:00"),
        rows(12, "2025-07-11 00:00:00"),
        rows(17, "2025-08-10 00:00:00"),
        rows(13, "2025-09-09 00:00:00"),
        rows(11, "2025-10-09 00:00:00"));
  }

  @Test
  public void testBytesTransferredOverTime() throws IOException {
    String query =
        String.format("source=%s | STATS sum(bytes) by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(bytes)", null, "bigint"),
        schema("span(`start`,30d)", null, "timestamp"));
    verifyDataRows(
        response,
        rows(385560, "2025-04-12 00:00:00"),
        rows(1470623, "2025-05-12 00:00:00"),
        rows(1326170, "2025-06-11 00:00:00"),
        rows(946422, "2025-07-11 00:00:00"),
        rows(826957, "2025-08-10 00:00:00"),
        rows(719758, "2025-09-09 00:00:00"),
        rows(643042, "2025-10-09 00:00:00"));
  }

  @Test
  public void testPacketsTransferredOverTime() throws IOException {
    String query =
        String.format("source=%s | STATS sum(packets) by span(`start`, 30d)", VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum(packets)", null, "bigint"),
        schema("span(`start`,30d)", null, "timestamp"));
    verifyDataRows(
        response,
        rows(360, "2025-04-12 00:00:00"),
        rows(1715, "2025-05-12 00:00:00"),
        rows(1396, "2025-06-11 00:00:00"),
        rows(804, "2025-07-11 00:00:00"),
        rows(941, "2025-08-10 00:00:00"),
        rows(890, "2025-09-09 00:00:00"),
        rows(709, "2025-10-09 00:00:00"));
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
        rows(267655, "10.0.113.54"),
        rows(259776, "11.111.108.48"),
        rows(214512, "223.252.77.226"),
        rows(210396, "10.0.194.75"),
        rows(192355, "10.0.11.144"),
        rows(187200, "120.67.35.74"),
        rows(183353, "10.0.167.74"),
        rows(182055, "10.0.74.110"),
        rows(176391, "10.0.3.220"),
        rows(175820, "10.0.83.167"));
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
        response,
        rows(267655, "121.65.198.154"),
        rows(259776, "10.0.91.27"),
        rows(214512, "10.0.165.194"),
        rows(210396, "6.186.106.13"),
        rows(192355, "182.53.30.77"),
        rows(187200, "10.0.163.249"),
        rows(183353, "30.193.135.22"),
        rows(182055, "213.227.231.57"),
        rows(176391, "39.40.182.87"),
        rows(175820, "10.0.14.9"));
  }

  @Test
  public void testTopTalkersByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(packets) as Packets by srcaddr | sort - Packets | head 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Packets", null, "bigint"), schema("srcaddr", null, "string"));
    verifyDataRows(
        response,
        rows(200, "10.0.163.249"),
        rows(199, "121.65.198.154"),
        rows(198, "10.0.91.27"),
        rows(197, "6.186.106.13"),
        rows(181, "115.27.64.3"),
        rows(181, "30.193.135.22"),
        rows(176, "10.0.227.35"),
        rows(174, "10.0.99.147"),
        rows(171, "10.0.231.176"),
        rows(164, "10.0.165.194"));
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
        rows(200, "120.67.35.74"),
        rows(199, "10.0.113.54"),
        rows(198, "11.111.108.48"),
        rows(197, "10.0.194.75"),
        rows(181, "10.0.167.74"),
        rows(181, "10.0.159.18"),
        rows(176, "10.0.62.137"),
        rows(174, "182.58.134.190"),
        rows(171, "34.55.235.91"),
        rows(164, "118.124.149.78"));
  }

  @Test
  public void testTopTalkersByIPs() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by srcaddr | SORT - Count | HEAD 10",
            VPC_FLOW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("srcaddr", null, "string"));
    verifyDataRows(
        response,
        rows(1, "1.24.59.183"),
        rows(1, "10.0.101.123"),
        rows(1, "10.0.107.121"),
        rows(1, "10.0.107.130"),
        rows(1, "10.0.108.29"),
        rows(1, "10.0.115.237"),
        rows(1, "10.0.117.121"),
        rows(1, "10.0.126.80"),
        rows(1, "10.0.13.162"),
        rows(1, "10.0.132.168"));
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
        rows(1, "10.0.100.62"),
        rows(1, "10.0.107.6"),
        rows(1, "10.0.109.2"),
        rows(1, "10.0.11.144"),
        rows(1, "10.0.113.54"),
        rows(1, "10.0.116.210"),
        rows(1, "10.0.118.54"),
        rows(1, "10.0.127.142"),
        rows(1, "10.0.138.175"),
        rows(1, "10.0.147.33"));
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
    assertEquals(100, response.getJSONArray("datarows").length());
  }
}
