package org.opensearch.sql.common.authinterceptors;

import com.amazonaws.ClientConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.authinterceptors.credentialsprovider.ClientConfigurationHelper;

@ExtendWith(MockitoExtension.class)
public class ClientConfigurationHelperTest {

  @Test
  void testGetConfusedDeputyConfiguration() {
    String[] clusterNameTuple = "123456789012:test".split(":");

    ClientConfiguration configurationWithConfusedDeputyHeaders
        = ClientConfigurationHelper.getConfusedDeputyConfiguration(
            clusterNameTuple, "us-east-1");

    Map<String, String> actualHeaders = configurationWithConfusedDeputyHeaders.getHeaders();
    Map<String, String> expectedHeaders = new HashMap<>() {{
        put(ClientConfigurationHelper.SOURCE_ARN_HEADER,
            String.format(ClientConfigurationHelper.OS_DOMAIN_ARN_FORMAT, "aws", "us-east-1",
                "123456789012", "test"));
        put(ClientConfigurationHelper.SOURCE_ACCOUNT_HEADER, "123456789012");
      }};

    Assertions.assertEquals(actualHeaders, expectedHeaders);
  }

  @Test
  void testGenerateDomainArn() {
    String[] clusterNameTuple = "123456789012:test".split(":");
    String actualDomainArn =
        ClientConfigurationHelper.generateDomainArn(clusterNameTuple, "us-east-1");
    String expectedDomainArn = String.format(ClientConfigurationHelper.OS_DOMAIN_ARN_FORMAT,
        "aws", "us-east-1", "123456789012", "test");

    Assertions.assertEquals(actualDomainArn, expectedDomainArn);
  }

  @Test
  void testGetPartion() {
    String partitionReturned = ClientConfigurationHelper.getPartition("us-east-1");
    Assertions.assertEquals("aws", partitionReturned);

    partitionReturned = ClientConfigurationHelper.getPartition("cn-northwest-1");
    Assertions.assertEquals("aws-cn", partitionReturned);

    partitionReturned = ClientConfigurationHelper.getPartition("aws-iso-us-east-1");
    Assertions.assertEquals("aws-iso", partitionReturned);

    partitionReturned = ClientConfigurationHelper.getPartition("aws-isob-us-west-2");
    Assertions.assertEquals("aws-iso-b", partitionReturned);
  }
}