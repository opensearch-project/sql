/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.util.List;
import lombok.AllArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.commons.authuser.User;
import org.opensearch.sql.datasource.DataSourceUserAuthorizationHelper;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

@AllArgsConstructor
public class DataSourceUserAuthorizationHelperImpl implements DataSourceUserAuthorizationHelper {
  private final Client client;

  private Boolean isAuthorizationRequired() {
    String userString = client.threadPool()
        .getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    return userString != null;
  }

  private List<String> getUserRoles() {
    String userString = client.threadPool()
        .getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    User user = User.parse(userString);
    return user.getRoles();
  }


  @Override
  public void authorizeDataSource(DataSourceMetadata dataSourceMetadata) {
    if (isAuthorizationRequired()
        && !dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      boolean isAuthorized = false;
      for (String role : getUserRoles()) {
        if (dataSourceMetadata.getAllowedRoles().contains(role)
            || role.equals("all_access")) {
          isAuthorized = true;
          break;
        }
      }
      if (!isAuthorized) {
        throw new SecurityException(
            String.format("User is not authorized to access datasource %s. "
                    + "User should be mapped to any of the roles in %s for access.",
                dataSourceMetadata.getName(), dataSourceMetadata.getAllowedRoles().toString()));
      }
    }
  }
}
