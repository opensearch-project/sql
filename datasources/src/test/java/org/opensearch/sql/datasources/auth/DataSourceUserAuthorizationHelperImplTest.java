/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.auth;

import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;

import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

@ExtendWith(MockitoExtension.class)
public class DataSourceUserAuthorizationHelperImplTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @InjectMocks
  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;


  @Test
  public void testAuthorizeDataSourceWithAllowedRoles() {
    String userString = "myuser|bckrole1,bckrol2|prometheus_access|myTenant";
    Mockito.when(client.threadPool().getThreadContext()
            .getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
        .thenReturn(userString);
    DataSourceMetadata dataSourceMetadata = dataSourceMetadata();
    this.dataSourceUserAuthorizationHelper
        .authorizeDataSource(dataSourceMetadata);
  }

  @Test
  public void testAuthorizeDataSourceWithAdminRole() {
    String userString = "myuser|bckrole1,bckrol2|all_access|myTenant";
    Mockito.when(client.threadPool().getThreadContext()
            .getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
        .thenReturn(userString);
    DataSourceMetadata dataSourceMetadata = dataSourceMetadata();
    this.dataSourceUserAuthorizationHelper
        .authorizeDataSource(dataSourceMetadata);
  }

  @Test
  public void testAuthorizeDataSourceWithNullUserString() {
    Mockito.when(client.threadPool().getThreadContext()
            .getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
        .thenReturn(null);
    DataSourceMetadata dataSourceMetadata = dataSourceMetadata();
    this.dataSourceUserAuthorizationHelper
        .authorizeDataSource(dataSourceMetadata);
  }

  @Test
  public void testAuthorizeDataSourceWithDefaultDataSource() {
    String userString = "myuser|bckrole1,bckrol2|role1|myTenant";
    Mockito.when(client.threadPool().getThreadContext()
            .getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
        .thenReturn(userString);
    DataSourceMetadata dataSourceMetadata =
        DataSourceMetadata.defaultOpenSearchDataSourceMetadata();
    this.dataSourceUserAuthorizationHelper
        .authorizeDataSource(dataSourceMetadata);
  }

  @Test
  public void testAuthorizeDataSourceWithException() {
    String userString = "myuser|bckrole1,bckrol2|role1|myTenant";
    Mockito.when(client.threadPool().getThreadContext()
            .getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
        .thenReturn(userString);
    DataSourceMetadata dataSourceMetadata = dataSourceMetadata();
    SecurityException securityException
        = Assert.assertThrows(SecurityException.class,
            () -> this.dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata));
    Assert.assertEquals("User is not authorized to access datasource test. "
            + "User should be mapped to any of the roles in [prometheus_access] for access.",
        securityException.getMessage());
  }

  private DataSourceMetadata dataSourceMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(List.of("prometheus_access"));
    dataSourceMetadata.setProperties(new HashMap<>());
    return dataSourceMetadata;
  }

}
