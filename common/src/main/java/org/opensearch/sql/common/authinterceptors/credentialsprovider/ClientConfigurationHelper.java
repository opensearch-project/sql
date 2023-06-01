/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.authinterceptors.credentialsprovider;

import com.amazonaws.ClientConfiguration;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClientConfigurationHelper {
  private ClientConfigurationHelper() {
    // no instance
  }

  public static final String SOURCE_ACCOUNT_HEADER = "x-amz-source-account";
  public static final String SOURCE_ARN_HEADER = "x-amz-source-arn";
  public static final String OS_DOMAIN_ARN_FORMAT = "arn:%s:es:%s:%s:domain/%s";

  private static final Logger LOGGER = LogManager.getLogger(ClientConfigurationHelper.class);

  /**
  * Forms ConfusedDeputy Configuration for the existing domain.
  *
  * @param clusterNameTuple clusterName.
  * @param region           region.
  * @return ClientConfiguration.
  */
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

  /**
   * This generates domain ARN of the opensearch instance.
   * 
   * @param clusterNameTuple clusterName Tuple.
   * @param region region.
   * @return domain ARN.
   */
  public static String generateDomainArn(String[] clusterNameTuple, String region) {
    String partition = getPartition(region);
    return String.format(OS_DOMAIN_ARN_FORMAT, partition, region, clusterNameTuple[0],
        clusterNameTuple[1]);
  }

  /**
   * Generates partition string.
   *
   * @param region region.
   * @return partition string.
   */
  public static String getPartition(String region) {
    final String partition = System.getenv("DOMAIN_PARTITION");

    if (!Strings.isNullOrEmpty(partition)) {
      return partition;
    }
    LOGGER.warn(
        "Domain Partition is missing from environment variable, "
            + "assuming partition on the basis of current region");
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
