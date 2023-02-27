/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.prometheus.authinterceptors.credentials;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Factory class that provides temporary credentials. It refreshes the credentials on demand.
 */
public class ExpirableCredentialsProviderFactory implements CredentialsProviderFactory {

  public ExpirableCredentialsProviderFactory(
      InternalAuthCredentialsClient internalAuthCredentialsClient,
      String[] clusterNameTuple
  ) {
    this.internalAuthCredentialsClient = internalAuthCredentialsClient;
    this.clusterNameTuple = clusterNameTuple;
  }

  /**
   * Provide expirable credentials.
   *
   * @param roleArn IAM role arn
   * @return AWSCredentialsProvider which holds the credentials.
   */
  @Override
  public AWSCredentialsProvider getProvider(String roleArn) {
    return getExpirableCredentialsProvider(roleArn);
  }

  private static final Logger LOG = LogManager.getLogger();

  private final InternalAuthCredentialsClient internalAuthCredentialsClient;
  private final String[] clusterNameTuple;

  private AWSCredentialsProvider getExpirableCredentialsProvider(String roleArn) {
    return findStsAssumeRoleCredentialsProvider(roleArn);
  }

  private AWSCredentialsProvider findStsAssumeRoleCredentialsProvider(String roleArn) {
    AWSCredentialsProvider assumeRoleApiCredentialsProvider = getAssumeRoleApiCredentialsProvider();

    if (assumeRoleApiCredentialsProvider != null) {
      LOG.info("Fetching credentials from STS for assumed role");
      return getStsAssumeCustomerRoleProvider(assumeRoleApiCredentialsProvider, roleArn);
    }
    LOG.error("Could not fetch credentials from internal service to assume role");
    return null;
  }

  private AWSCredentialsProvider getAssumeRoleApiCredentialsProvider() {
    InternalAuthApiCredentialsProvider internalAuthApiCredentialsProvider =
        new InternalAuthApiCredentialsProvider(
            internalAuthCredentialsClient,
            InternalAuthApiCredentialsProvider.POLICY_TYPES.get("ASSUME_ROLE"));

    return internalAuthApiCredentialsProvider.getCredentials() != null
        ? internalAuthApiCredentialsProvider : null;
  }

  private AWSCredentialsProvider getStsAssumeCustomerRoleProvider(
      AWSCredentialsProvider apiCredentialsProvider, String roleArn) {
    String region = "us-east-1";
//    try {
//      region = EC2MetadataUtils.getEC2InstanceRegion();
//    } catch (Throwable ex) {
//      LOG.info(
//          "Exception occurred while fetching the region info from EC2 metadata. "
//              + "Defaulting to us-east-1");
//    }
    LOG.info("Clustername Tuple: " + Arrays.toString(clusterNameTuple));
    LOG.info("Region" + region);
    final ClientConfiguration configurationWithConfusedDeputyHeaders =
        ClientConfigurationHelper.getConfusedDeputyConfiguration(clusterNameTuple, region);
    AWSSecurityTokenServiceClientBuilder stsClientBuilder =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(apiCredentialsProvider)
            .withClientConfiguration(configurationWithConfusedDeputyHeaders)
            .withRegion(region);
    AWSSecurityTokenService stsClient = stsClientBuilder.build();
    LOG.info("Built stsclient");
    STSAssumeRoleSessionCredentialsProvider.Builder providerBuilder =
        new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "sql")
            .withStsClient(stsClient);
    LOG.info("Built credentials provider");
    return providerBuilder.build();
  }
}
