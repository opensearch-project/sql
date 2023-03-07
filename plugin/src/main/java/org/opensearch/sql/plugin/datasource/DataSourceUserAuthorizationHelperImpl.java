/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;

import java.util.List;
import lombok.AllArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.commons.authuser.User;
import org.opensearch.sql.datasource.DataSourceUserAuthorizationHelper;

@AllArgsConstructor
public class DataSourceUserAuthorizationHelperImpl implements DataSourceUserAuthorizationHelper {
  private final Client client;

  @Override
  public Boolean isAuthorizationRequired() {
    String userString = client.threadPool()
        .getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    return userString != null;
  }

  @Override
  public List<String> getUserRoles() {
    String userString = client.threadPool()
        .getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    User user = User.parse(userString);
    return user.getRoles();
  }
}
