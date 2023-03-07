package org.opensearch.sql.datasource;

import java.util.List;

/**
 * Interface for datasource authorization helper.
 * The implementation of this class helps in determining
 * if authorization is required and the roles associated with the user.
 */
public interface DataSourceUserAuthorizationHelper {

  /**
   * Returns if authorization is required within the current context of opensearch domain.
   * If security plugin is enabled, this method will return true or else it will return false.
   *
   * @return Boolean.True if authorization is required or else Boolean.False.
   */
  Boolean isAuthorizationRequired();

  /**
   * Returns OpenSearch Security roles associated with the current auth user in context.
   *
   * @return list of user roles {@link List}
   */
  List<String> getUserRoles();

}
