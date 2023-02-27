package org.opensearch.sql.prometheus.authinterceptors.credentials;

import com.amazonaws.ClientConfiguration;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClientConfigurationHelper {
  private ClientConfigurationHelper() {
    // no instance
  }

  protected static final String SOURCE_ACCOUNT_HEADER = "x-amz-source-account";
  protected static final String SOURCE_ARN_HEADER = "x-amz-source-arn";
  protected static final String OS_DOMAIN_ARN_FORMAT = "arn:%s:es:%s:%s:domain/%s";

  private final static Logger LOGGER = LogManager.getLogger(ClientConfigurationHelper.class);

  public static ClientConfiguration getConfusedDeputyConfiguration(String[] clusterNameTuple,
                                                                   String region) {
    String clientId = clusterNameTuple[0];
    String domainArn = generateDomainArn(clusterNameTuple, region);

    // Confused Deputy Protection Requirement
    // https://w.amazon.com/bin/view/AWSAuth/AccessManagement/Resource_Policy_Confused_Deputy_Protection
    LOGGER.debug("Adding Source ARN " + domainArn + " and Source Account " + clientId
        + " in request headers for Confused Deputy Protection");
    return new ClientConfiguration().withHeader(SOURCE_ARN_HEADER, domainArn)
        .withHeader(SOURCE_ACCOUNT_HEADER, clientId);
  }

  protected static String generateDomainArn(String[] clusterNameTuple, String region) {
    String partition = getPartition(region);
    return String.format(OS_DOMAIN_ARN_FORMAT, partition, region, clusterNameTuple[0],
        clusterNameTuple[1]);
  }

  protected static String getPartition(String region) {
    final String partition = System.getenv("DOMAIN_PARTITION");

    if (!Strings.isNullOrEmpty(partition)) {
      return partition;
    }
    LOGGER.warn(
        "Domain Partition is missing from environment variable, assuming partition on the basis of current region");
    if (region.contains("gov")) {
      return "aws-us-gov";
    }
    if (region.contains("-isob-")) {
      return "aws-iso-b";
    }
    if (region.contains("-iso-")) {
      return "aws-iso";
    }
    if (region.startsWith("cn-")) {
      return "aws-cn";
    }
    return "aws";
  }
}