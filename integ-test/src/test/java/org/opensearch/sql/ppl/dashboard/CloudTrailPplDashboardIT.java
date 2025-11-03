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

/**
 * Integration tests for CloudTrail PPL dashboard queries using exact original dashboard query
 * format. These tests ensure that CloudTrail-related PPL queries work correctly with actual test
 * data.
 */
public class CloudTrailPplDashboardIT extends PPLIntegTestCase {

  private static final String CLOUDTRAIL_LOGS_INDEX = "cloudtrail_logs";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadCloudTrailLogsIndex();
  }

  private void loadCloudTrailLogsIndex() throws IOException {
    if (!TestUtils.isIndexExist(client(), CLOUDTRAIL_LOGS_INDEX)) {
      String mapping =
          TestUtils.getMappingFile("doctest/mappings/cloudtrail_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), CLOUDTRAIL_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(
          client(),
          CLOUDTRAIL_LOGS_INDEX,
          "src/test/resources/doctest/testdata/cloudtrail_logs.json");
    }
  }

  @Test
  public void testTotalEventsCount() throws IOException {
    String query =
        String.format("source=%s | stats count() as `Event Count`", CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Event Count", null, "bigint"));
    verifyDataRows(response, rows(100));
  }

  @Test
  public void testEventsOverTime() throws IOException {
    String query =
        String.format("source=%s | stats count() by span(start_time, 30d)", CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("count()", null, "bigint"),
        schema("span(start_time,30d)", null, "timestamp"));
  }

  @Test
  public void testEventsByAccountIds() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(userIdentity.accountId) | stats count() as Count by"
                + " userIdentity.accountId | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("userIdentity.accountId", null, "string"));
  }

  @Test
  public void testEventsByCategory() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by eventCategory | sort - Count | head 5",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("eventCategory", null, "string"));
    verifyDataRows(response, rows(100, "Management"));
  }

  @Test
  public void testEventsByRegion() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `awsRegion` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("awsRegion", null, "string"));
    verifyDataRows(
        response,
        rows(12, "us-west-1"),
        rows(12, "ca-central-1"),
        rows(9, "us-west-2"),
        rows(8, "ap-southeast-1"),
        rows(8, "ap-northeast-1"),
        rows(7, "us-east-2"),
        rows(7, "sa-east-1"),
        rows(7, "eu-north-1"),
        rows(7, "ap-south-1"),
        rows(6, "ap-southeast-2"));
  }

  @Test
  public void testTop10EventAPIs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventName` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventName", null, "string"));
  }

  @Test
  public void testTop10Services() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventSource` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventSource", null, "string"));
    verifyDataRows(
        response,
        rows(15, "ec2.amazonaws.com"),
        rows(14, "s3.amazonaws.com"),
        rows(13, "rds.amazonaws.com"),
        rows(10, "dynamodb.amazonaws.com"),
        rows(9, "cloudwatch.amazonaws.com"),
        rows(8, "sts.amazonaws.com"),
        rows(8, "lambda.amazonaws.com"),
        rows(8, "iam.amazonaws.com"),
        rows(8, "cloudformation.amazonaws.com"),
        rows(7, "logs.amazonaws.com"));
  }

  @Test
  public void testTop10SourceIPs() throws IOException {
    String query =
        String.format(
            "source=%s | WHERE NOT (sourceIPAddress LIKE '%%amazon%%.com%%') | STATS count() as"
                + " Count by sourceIPAddress| SORT - Count| HEAD 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("sourceIPAddress", null, "string"));
  }

  @Test
  public void testTop10UsersGeneratingEvents() throws IOException {
    String query =
        String.format(
            "source=%s | where ISNOTNULL(`userIdentity.accountId`)| STATS count() as Count by"
                + " `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.accountId`,"
                + " `userIdentity.sessionContext.sessionIssuer.type` | rename"
                + " `userIdentity.sessionContext.sessionIssuer.userName` as `User Name`,"
                + " `userIdentity.accountId` as `Account Id`,"
                + " `userIdentity.sessionContext.sessionIssuer.type` as `Type` | SORT - Count |"
                + " HEAD 1000",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("User Name", null, "string"),
        schema("Account Id", null, "string"),
        schema("Type", null, "string"));
  }

  @Test
  public void testEC2ChangeEventCount() throws IOException {
    String query =
        String.format(
            "source=%s | where eventSource like \\\"ec2%%\\\" and (eventName = \\\"RunInstances\\\""
                + " or eventName = \\\"TerminateInstances\\\" or eventName = \\\"StopInstances\\\")"
                + " and not (eventName like \\\"Get%%\\\" or eventName like \\\"Describe%%\\\" or"
                + " eventName like \\\"List%%\\\" or eventName like \\\"Head%%\\\") | stats count()"
                + " as Count by eventName | sort - Count | head 5",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventName", null, "string"));
    verifyDataRows(response, rows(1, "TerminateInstances"), rows(1, "RunInstances"));
  }

  @Test
  public void testEC2UsersBySessionIssuer() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`userIdentity.sessionContext.sessionIssuer.userName`) and"
                + " `eventSource` like 'ec2%%' and not (`eventName` like 'Get%%' or `eventName`"
                + " like 'Describe%%' or `eventName` like 'List%%' or `eventName` like 'Head%%') |"
                + " stats count() as Count by `userIdentity.sessionContext.sessionIssuer.userName`"
                + " | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("userIdentity.sessionContext.sessionIssuer.userName", null, "string"));
    verifyDataRows(
        response, rows(1, "Analyst"), rows(1, "DataEngineer"), rows(1, "ec2-service"), rows(1, ""));
  }

  @Test
  public void testEC2EventsByName() throws IOException {
    String query =
        String.format(
            "source=%s | where `eventSource` like \\\"ec2%%\\\" and not (`eventName` like"
                + " \\\"Get%%\\\" or `eventName` like \\\"Describe%%\\\" or `eventName` like"
                + " \\\"List%%\\\" or `eventName` like \\\"Head%%\\\") | stats count() as Count by"
                + " `eventName` | rename `eventName` as `Event Name` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("Event Name", null, "string"));
    verifyDataRows(
        response,
        rows(2, "CreateSecurityGroup"),
        rows(1, "RunInstances"),
        rows(1, "TerminateInstances"));
  }
}
