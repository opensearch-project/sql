/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration tests for CloudTrail PPL dashboard queries. These tests ensure that
 * CloudTrail-related PPL queries work correctly with actual test data.
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
      String mapping = TestUtils.getMappingFile("cloudtrail_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), CLOUDTRAIL_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(
          client(), CLOUDTRAIL_LOGS_INDEX, "src/test/resources/cloudtrail_logs.json");
    }
  }

  @Test
  public void testTotalEventsCount() throws IOException {
    String query =
        String.format("source=%s | stats count() as `Event Count`", CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Event Count", null, "bigint"));
    verifyDataRows(response, rows(4));
  }

  @Test
  public void testEventsOverTime() throws IOException {
    String query = String.format("source=%s | stats count() by `eventTime`", CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("count()", null, "bigint"), schema("eventTime", null, "timestamp"));
    verifyDataRows(
        response,
        rows(1, "2024-09-18 15:15:08"),
        rows(1, "2024-09-18 15:16:54"),
        rows(2, "2024-09-18 15:18:05"));
  }

  @Test
  public void testEventsByAccountIds() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`userIdentity.accountId`) | stats count() as Count by"
                + " `userIdentity.accountId` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("userIdentity.accountId", null, "string"));
    verifyDataRows(response, rows(1, "481665107626"));
  }

  @Test
  public void testEventsByCategory() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventCategory` | sort - Count | head 5",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("eventCategory", null, "string"));
    verifyDataRows(response, rows(4, "Management"));
  }

  @Test
  public void testEventsByRegion() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `awsRegion` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("awsRegion", null, "string"));
    verifyDataRows(response, rows(4, "us-west-2"));
  }

  @Test
  public void testTop10EventAPIs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventName` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventName", null, "string"));
    verifyDataRows(
        response,
        rows(1, "AssumeRole"),
        rows(1, "GenerateDataKey"),
        rows(1, "GetBucketAcl"),
        rows(1, "ListLogFiles"));
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
        rows(1, "sts.amazonaws.com"),
        rows(1, "kms.amazonaws.com"),
        rows(1, "s3.amazonaws.com"),
        rows(1, "logs.amazonaws.com"));
  }

  @Test
  public void testTop10SourceIPs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `sourceIPAddress` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("sourceIPAddress", null, "string"));
    verifyDataRows(
        response,
        rows(2, "cloudtrail.amazonaws.com"),
        rows(1, "scheduler.amazonaws.com"),
        rows(1, "directquery.opensearchservice.amazonaws.com"));
  }

  @Test
  public void testTop10UsersGeneratingEvents() throws IOException {
    String query =
        String.format(
            "source=%s | where isnotnull(`userIdentity.sessionContext.sessionIssuer.userName`) and"
                + " isnotnull(`userIdentity.sessionContext.sessionIssuer.arn`) and"
                + " isnotnull(`userIdentity.accountId`) and"
                + " isnotnull(`userIdentity.sessionContext.sessionIssuer.type`) | stats count() as"
                + " Count by `userIdentity.sessionContext.sessionIssuer.userName`,"
                + " `userIdentity.accountId`, `userIdentity.sessionContext.sessionIssuer.type`,"
                + " `userIdentity.sessionContext.sessionIssuer.arn` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("userIdentity.sessionContext.sessionIssuer.userName", null, "string"),
        schema("userIdentity.accountId", null, "string"),
        schema("userIdentity.sessionContext.sessionIssuer.type", null, "string"),
        schema("userIdentity.sessionContext.sessionIssuer.arn", null, "string"));
    verifyDataRows(
        response,
        rows(
            1,
            "NexusIntegrationStack-DirectQueryForAmazonOpenSearc-iIy4asCVtbwc",
            "481665107626",
            "Role",
            "arn:aws:iam::481665107626:role/NexusIntegrationStack-DirectQueryForAmazonOpenSearc-iIy4asCVtbwc"));
  }

  @Test
  public void testTopEventNames() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventName` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventName", null, "string"));
    verifyDataRows(
        response,
        rows(1, "AssumeRole"),
        rows(1, "GenerateDataKey"),
        rows(1, "GetBucketAcl"),
        rows(1, "ListLogFiles"));
  }

  @Test
  public void testTopEventSources() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `eventSource` | sort - Count | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("eventSource", null, "string"));
    verifyDataRows(
        response,
        rows(1, "sts.amazonaws.com"),
        rows(1, "kms.amazonaws.com"),
        rows(1, "s3.amazonaws.com"),
        rows(1, "logs.amazonaws.com"));
  }

  @Test
  public void testS3AccessDenied() throws IOException {
    String query =
        String.format(
            "source=%s | where `eventSource` like 's3%%' and `errorCode`='AccessDenied' | stats"
                + " count() as Count",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"));
    verifyDataRows(response, rows(0));
  }

  @Test
  public void testS3Buckets() throws IOException {
    String query =
        String.format(
            "source=%s | where `eventSource` like 's3%%' and"
                + " isnotnull(`requestParameters.bucketName`) | stats count() as Bucket by"
                + " `requestParameters.bucketName` | sort - Bucket | head 10",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Bucket", null, "bigint"),
        schema("requestParameters.bucketName", null, "string"));
    verifyDataRows(response, rows(1, "aws-cloudtrail-logs-481665107626-69fb0771"));
  }

  @Test
  public void testTopS3ChangeEvents() throws IOException {
    String query =
        String.format(
            "source=%s | where `eventSource` = 's3.amazonaws.com' and"
                + " isnotnull(`requestParameters.bucketName`) and not like(`eventName`, 'Get%%')"
                + " and not like(`eventName`, 'Describe%%') and not like(`eventName`, 'List%%') and"
                + " not like(`eventName`, 'Head%%') | stats count() as Count",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"));
    verifyDataRows(response, rows(0));
  }

  @Test
  public void testEC2ChangeEventCount() throws IOException {
    String query =
        String.format(
            "source=%s | where `eventSource` = 'ec2.amazonaws.com' and (`eventName` ="
                + " 'RunInstances' or `eventName` = 'TerminateInstances' or `eventName` ="
                + " 'StopInstances') and not like(`eventName`, 'Get%%') and not like(`eventName`,"
                + " 'Describe%%') and not like(`eventName`, 'List%%') and not like(`eventName`,"
                + " 'Head%%') | stats count() as Count",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"));
    verifyDataRows(response, rows(0));
  }

  @Test
  public void testErrorEvents() throws IOException {
    String query =
        String.format(
            "source=%s | fields `eventTime`, `errorCode`, `eventName`, `eventSource`,"
                + " `userIdentity.sessionContext.sessionIssuer.userName`,"
                + " `userIdentity.sessionContext.sessionIssuer.accountId`,"
                + " `userIdentity.sessionContext.sessionIssuer.arn`,"
                + " `userIdentity.sessionContext.sessionIssuer.type`, `awsRegion`,"
                + " `sourceIPAddress`, `userIdentity.accountId` | sort - `eventTime`",
            CLOUDTRAIL_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    assertEquals(4, response.getJSONArray("datarows").length());
  }
}
