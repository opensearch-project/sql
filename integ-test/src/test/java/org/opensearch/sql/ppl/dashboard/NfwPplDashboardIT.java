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
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class NfwPplDashboardIT extends PPLIntegTestCase {

  private static final String NFW_LOGS_INDEX = "nfw_logs";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadNfwLogsIndex();
  }

  private void loadNfwLogsIndex() throws IOException {
    if (TestUtils.isIndexExist(client(), NFW_LOGS_INDEX)) {
      Request deleteRequest = new Request("DELETE", "/" + NFW_LOGS_INDEX);
      TestUtils.performRequest(client(), deleteRequest);
    }
    String mapping = TestUtils.getMappingFile("mappings/nfw_logs_index_mapping.json");
    TestUtils.createIndexByRestClient(client(), NFW_LOGS_INDEX, mapping);
    TestUtils.loadDataByRestClient(
        client(),
        NFW_LOGS_INDEX,
        "src/test/java/org/opensearch/sql/ppl/dashboard/testdata/nfw_logs.json");
  }

  @Test
  public void testTopApplicationProtocols() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`event.app_proto`) | STATS count() as Count by"
                + " `event.app_proto` | SORT - Count| HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", "bigint"), schema("event.app_proto", "string"));
    verifyDataRows(
        response, rows(89L, "http"), rows(5L, "unknown"), rows(2L, "tls"), rows(2L, "dns"));
  }

  @Test
  public void testTopSourceIPByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`,"
                + " 2d) as timestamp_span, `event.src_ip` | rename `event.src_ip` as `Source IP` |"
                + " sort - packet_count | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("packet_count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("Source IP", "string"));
  }

  @Test
  public void testTopSourceIPByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.bytes`) as sum_bytes by span(`event.timestamp`,"
                + " 2d) as timestamp_span, `event.src_ip` | rename  `event.src_ip` as `Source IP` |"
                + " sort - sum_bytes | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("sum_bytes", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("Source IP", "string"));
  }

  @Test
  public void testTopDestinationIPByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`,"
                + " 2d) as timestamp_span, `event.dest_ip` | rename `event.dest_ip` as"
                + " `Destination IP` | sort - packet_count | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("packet_count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("Destination IP", "string"));
  }

  @Test
  public void testTopDestinationIPByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.bytes`) as bytes by span(`event.timestamp`, 2d)"
                + " as timestamp_span, `event.dest_ip` | rename `event.dest_ip` as `Destination IP`"
                + " | sort - bytes| head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("bytes", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("Destination IP", "string"));
  }

  @Test
  public void testTopSourceIPsByPacketsAndBytes() throws IOException {
    String query =
        String.format(
            "source=%s | STATS SUM(`event.netflow.pkts`) as Packets, SUM(`event.netflow.bytes`) as"
                + " Bytes by `event.src_ip` | RENAME `event.src_ip` as `Source IP` | SORT - Bytes,"
                + " Packets | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Packets", "bigint"),
        schema("Bytes", "bigint"),
        schema("Source IP", "string"));
  }

  @Test
  public void testTopDestinationIPsByPacketsAndBytes() throws IOException {
    String query =
        String.format(
            "source=%s | STATS SUM(`event.netflow.pkts`) as Packets, SUM(`event.netflow.bytes`) as"
                + " Bytes by `event.dest_ip`| RENAME `event.dest_ip` as `Destination IP` | SORT -"
                + " Bytes, Packets| HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Packets", "bigint"),
        schema("Bytes", "bigint"),
        schema("Destination IP", "string"));
  }

  @Test
  public void testTopSourceAndDestinationPackets() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`,"
                + " 2d) as timestamp_span, `event.src_ip`, `event.dest_ip` |  eval `Src IP - Dst"
                + " IP` = concat(`event.src_ip`, \\\"-\\\", `event.dest_ip`) | sort - packet_count"
                + " | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("packet_count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("Src IP - Dst IP", "string"));
  }

  @Test
  public void testTopSourceAndDestinationByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(`event.netflow.bytes`) as bytes by span(`event.timestamp`, 2d)"
                + " as timestamp_span, `event.src_ip`, `event.dest_ip` | eval `Src IP - Dst IP` ="
                + " concat(`event.src_ip`, \\\"-\\\", `event.dest_ip`) | sort - bytes | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("bytes", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("Src IP - Dst IP", "string"));
  }

  @Test
  public void testTopHTTPHostHeaders() throws IOException {
    String query =
        String.format(
            "source=%s | where `event.alert.action` ="
                + " \\\"allowed\\\"| stats count() as event_count by span(`event.timestamp`, 2d) as"
                + " time_bucket, `event.http.hostname` | rename `event.http.hostname` as"
                + " `Hostname`| sort - event_count",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("time_bucket", "timestamp"),
        schema("Hostname", "string"));
    verifyDataRows(response, rows(1L, "2025-03-27 00:00:00", null));
  }

  @Test
  public void testTopBlockedHTTPHostHeaders() throws IOException {
    String query =
        String.format(
            "source=%s | where `event.alert.action` = \\\"blocked\\\" and"
                + " isnotnull(`event.http.hostname`) | stats count() as event_count by"
                + " span(`event.timestamp`, 2d) as time_bucket, `event.http.hostname` | rename"
                + " `event.http.hostname` as `Hostname` | sort - event_count |"
                + " HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("time_bucket", "timestamp"),
        schema("Hostname", "string"));
    verifyDataRows(response, rows(1L, "2025-02-23 00:00:00", "checkip.amazonaws.com"));
  }

  @Test
  public void testTopAllowedTLSSNI() throws IOException {
    String query =
        String.format(
            "source=%s | where `event.alert.action` = \\\"allowed\\\"| stats count() as event_count"
                + " by span(`event.timestamp`, 2d) as time_bucket, `event.tls.sni`| rename"
                + " `event.tls.sni` as `Hostname` | sort - event_count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("time_bucket", "timestamp"),
        schema("Hostname", "string"));
    verifyDataRows(response, rows(1L, "2025-03-27 00:00:00", null));
  }

  @Test
  public void testTopBlockedTLSSNI() throws IOException {
    String query =
        String.format(
            "source=%s | where `event.alert.action` = \\\"blocked\\\" and"
                + " isnotnull(`event.tls.sni`)| stats count() as event_count by"
                + " span(`event.timestamp`, 2d) as time_bucket, `event.tls.sni` | rename"
                + " `event.tls.sni` as `Hostname` | sort - event_count| HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("time_bucket", "timestamp"),
        schema("Hostname", "string"));
    verifyDataRows(
        response,
        rows(1L, "2025-02-23 00:00:00", "checkip.amazonaws.com"),
        rows(1L, "2025-03-27 00:00:00", "s3.us-east-1.amazonaws.com"));
  }

  @Test
  public void testTopHTTPURIPaths() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`event.http.url`)| stats count() as event_count by"
                + " span(`event.timestamp`, 2d) as timestamp_span, `event.http.url`| rename"
                + " `event.http.url` as `URL` | sort - event_count| head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("URL", "string"));
    verifyDataRows(response, rows(1L, "2025-02-23 00:00:00", "/"));
  }

  @Test
  public void testTopHTTPUserAgents() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`event.http.http_user_agent`) | stats count() as"
                + " event_count by span(`event.timestamp`, 2d) as timestamp_span,"
                + " `event.http.http_user_agent` | rename `event.http.http_user_agent` as `User"
                + " Agent` | sort - event_count| head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("event_count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("User Agent", "string"));
    verifyDataRows(response, rows(1L, "2025-02-23 00:00:00", "curl/8.5.0"));
  }

  @Test
  public void testTopPrivateLinkEndpointCandidates() throws IOException {
    String query =
        String.format(
            "source=%s | where (`event.tls.sni` like 's3%%') or (`event.http.hostname` like"
                + " 's3%%') or (`event.tls.sni` like 'dynamodb%%') or (`event.http.hostname` like"
                + " 'dynamodb%%') or (`event.tls.sni` like 'backup%%') or (`event.http.hostname`"
                + " like 'backup%%')| STATS count() as Count by `event.src_ip`, `event.dest_ip`,"
                + " `event.app_proto`, `event.tls.sni`, `event.http.hostname` | rename"
                + " `event.tls.sni` as SNI, `event.dest_ip` as Dest_IP , `event.src_ip` as"
                + " Source_IP, `event.http.hostname` as Hostname, `event.app_proto` as App_Proto |"
                + " SORT - Count",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("SNI", "string"),
        schema("Dest_IP", "string"),
        schema("Source_IP", "string"),
        schema("Hostname", "string"),
        schema("App_Proto", "string"));
    verifyDataRows(
        response,
        rows(1L, "10.2.1.120", "52.216.211.88", "tls", "s3.us-east-1.amazonaws.com", null));
  }

  @Test
  public void testTopProtocols() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by `event.proto`| SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", "bigint"), schema("event.proto", "string"));
    verifyDataRows(response, rows(95L, "TCP"), rows(2L, "UDP"), rows(3L, "ICMP"));
  }

  @Test
  public void testTopSourcePorts() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span,"
                + " `event.src_port` | eval `Source Port` = CAST(`event.src_port` AS STRING) | sort"
                + " - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_port", "bigint"),
        schema("Source Port", "string"));
  }

  @Test
  public void testTopDestinationPorts() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span,"
                + " `event.dest_port` | eval `Destination Port` = CAST(`event.dest_port` AS STRING)"
                + " | sort - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.dest_port", "bigint"),
        schema("Destination Port", "string"));
  }

  @Test
  public void testTopTCPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"TCP\\\" | STATS count() as Count by"
                + " SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`,"
                + " `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, \\\" -"
                + " \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) | SORT -"
                + " Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopTCPFlowsByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"TCP\\\" | STATS sum(`event.netflow.pkts`) as"
                + " Packets by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` ="
                + " CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Packets | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Packets", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopTCPFlowsByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"TCP\\\" | STATS sum(event.netflow.bytes) as"
                + " Bytes by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` ="
                + " CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Bytes | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Bytes", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopTCPFlags() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by `event.tcp.tcp_flags` | SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", "bigint"), schema("event.tcp.tcp_flags", "string"));
    verifyDataRows(
        response,
        rows(8L, null),
        rows(4L, "13"),
        rows(4L, "17"),
        rows(3L, "0"),
        rows(3L, "1"),
        rows(3L, "15"),
        rows(3L, "16"),
        rows(3L, "18"),
        rows(3L, "19"),
        rows(3L, "2"));
  }

  @Test
  public void testTopUDPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"UDP\\\"| STATS count() as Count by"
                + " SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`,"
                + " `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, \\\" -"
                + " \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) | SORT -"
                + " Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopUDPFlowsByPackets() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"UDP\\\" | STATS sum(`event.netflow.pkts`) as"
                + " Packets by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` ="
                + " CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Packets | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Packets", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopUDPFlowsByBytes() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"UDP\\\" | STATS sum(`event.netflow.bytes`) as"
                + " Bytes by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` ="
                + " CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Bytes | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Bytes", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopICMPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = \\\"ICMP\\\" | STATS count() as Count by"
                + " SPAN(`event.timestamp`, 1d) as timestamp_span, `event.src_ip`, `event.dest_ip`,"
                + " `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, \\\" -"
                + " \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) | SORT -"
                + " Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopDropRejectRules() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = \\\"blocked\\\"| STATS count() as Count by"
                + " `event.alert.signature_id`, `event.alert.action`, `event.alert.signature`,"
                + " `event.proto`| RENAME  `event.alert.signature_id` as SID, `event.alert.action`"
                + " as Action, `event.alert.signature` as Message, `event.proto` as Proto | SORT -"
                + " Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("SID", "bigint"),
        schema("Action", "string"),
        schema("Message", "string"),
        schema("Proto", "string"));
  }

  @Test
  public void testTopAllowedRules() throws IOException {
    String query =
        String.format(
            "source=%s | where `event.alert.action` = \\\"allowed\\\" | stats count() as Count by"
                + " `event.alert.signature_id`, `event.alert.action`, `event.alert.signature`,"
                + " `event.proto` | rename `event.alert.signature_id` as SID, `event.alert.action`"
                + " as Action, `event.alert.signature` as Message, `event.proto` as Proto | sort -"
                + " Count | head 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("SID", "bigint"),
        schema("Action", "string"),
        schema("Message", "string"),
        schema("Proto", "string"));
  }

  @Test
  public void testTopBlockedSourceIPs() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = \\\"blocked\\\" | STATS COUNT() as Count by"
                + " `event.src_ip` | SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", "bigint"), schema("event.src_ip", "string"));
    verifyDataRows(response, rows(4L, "10.170.18.235"), rows(1L, "10.2.1.120"));
  }

  @Test
  public void testTopBlockedDestinationIPs() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = \\\"blocked\\\" | STATS COUNT() as Count by"
                + " `event.dest_ip` | SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", "bigint"), schema("event.dest_ip", "string"));
    verifyDataRows(
        response,
        rows(2L, "8.8.8.8"),
        rows(1L, "54.146.42.172"),
        rows(1L, "54.242.115.112"),
        rows(1L, "52.216.211.88"));
  }

  @Test
  public void testTopBlockedDestinationPorts() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = \\\"blocked\\\" | STATS COUNT() as `Count` by"
                + " `event.dest_port` | EVAL `Destination Port` = CAST(`event.dest_port` as STRING)"
                + " | SORT - `Count` | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("event.dest_port", "bigint"),
        schema("Destination Port", "string"));
  }

  @Test
  public void testTopBlockedRemoteAccessPorts() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = \\\"blocked\\\" | STATS count() as Count by"
                + " SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`,"
                + " `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, \\\" -"
                + " \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) | SORT -"
                + " Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopBlockedTCPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = 'blocked' and `event.proto` = 'TCP' | STATS"
                + " count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span,"
                + " `event.src_ip`, `event.dest_ip`, `event.dest_port`| EVAL `Src IP - Dst IP:Port`"
                + " = CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopBlockedUDPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.alert.action` = 'blocked' and `event.proto` = 'UDP' | STATS"
                + " count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span,"
                + " `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst"
                + " IP:Port` = CONCAT(`event.src_ip`, \\\" - \\\", `event.dest_ip`, \\\": \\\","
                + " CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP - Dst IP:Port", "string"));
  }

  @Test
  public void testTopTCPFlowsSynWithoutSynAck() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = 'TCP' and `event.tcp.syn` = \\\"true\\\" and"
                + " `event.tcp.ack` = \\\"true\\\" | STATS count() as Count by"
                + " SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.src_port`, `event.dest_ip`, `event.dest_port`| EVAL `Src IP:Port - Dst"
                + " IP:Port` = CONCAT(`event.src_ip`, \\\": \\\", CAST(`event.src_port` AS STRING),"
                + " \\\" - \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) |"
                + " SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.src_port", "bigint"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP:Port - Dst IP:Port", "string"));
  }

  @Test
  public void testTopLongLivedTCPFlows() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE `event.proto` = 'TCP' and `event.netflow.age` > 350 | STATS count()"
                + " as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`,"
                + " `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst"
                + " IP:Port` = CONCAT(`event.src_ip`, \\\": \\\", CAST(`event.src_port` AS STRING),"
                + " \\\" - \\\", `event.dest_ip`, \\\": \\\", CAST(`event.dest_port` AS STRING)) |"
                + " SORT - Count | HEAD 10",
            NFW_LOGS_INDEX);
    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", "bigint"),
        schema("timestamp_span", "timestamp"),
        schema("event.src_ip", "string"),
        schema("event.src_port", "bigint"),
        schema("event.dest_ip", "string"),
        schema("event.dest_port", "bigint"),
        schema("Src IP:Port - Dst IP:Port", "string"));
    verifyDataRows(
        response,
        rows(
            1L,
            "2025-03-27 00:00:00",
            "45.82.78.100",
            52610L,
            "10.2.1.120",
            8085L,
            "45.82.78.100: 52610 - 10.2.1.120: 8085"),
        rows(
            1L,
            "2025-03-27 00:00:00",
            "20.65.193.116",
            45550L,
            "10.2.1.120",
            1433L,
            "20.65.193.116: 45550 - 10.2.1.120: 1433"));
  }
}
