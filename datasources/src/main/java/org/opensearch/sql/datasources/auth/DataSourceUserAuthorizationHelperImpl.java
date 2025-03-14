/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.auth;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.util.List;
import lombok.AllArgsConstructor;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.transport.client.Client;

@AllArgsConstructor
public class DataSourceUserAuthorizationHelperImpl implements DataSourceUserAuthorizationHelper {
  private final Client client;

  private Boolean isAuthorizationRequired() {
    String userString =
        client
            .threadPool()
            .getThreadContext()
            .getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    return userString != null;
  }

  private List<String> getUserRoles() {
    String userString =
        client
            .threadPool()
            .getThreadContext()
            .getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    User user = User.parse(userString);
    return user.getRoles();
  }

  @Override
  public void authorizeDataSource(DataSourceMetadata dataSourceMetadata) {
    if (isAuthorizationRequired()
        && !dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      boolean isAuthorized = false;
      for (String role : getUserRoles()) {
        if (dataSourceMetadata.getAllowedRoles().contains(role) || role.equals("all_access")) {
          isAuthorized = true;
          break;
        }
      }
      if (!isAuthorized) {
        throw new OpenSearchSecurityException(
            String.format(
                "User is not authorized to access datasource %s. "
                    + "User should be mapped to any of the roles in %s for access.",
                dataSourceMetadata.getName(), dataSourceMetadata.getAllowedRoles().toString()),
            RestStatus.UNAUTHORIZED);
      }
    }
  }
}
