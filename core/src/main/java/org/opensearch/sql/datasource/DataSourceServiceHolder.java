package org.opensearch.sql.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * This class is created to solve the problem in binding implementation classes to interface.
 * For eg: If we return KeyStoreDataServiceImpl from createComponents in SQLPlugin.
 * It will bind the instance to KeyStoreDataServiceImpl instead of DataSourceService.
 * <a href="https://github.com/opensearch-project/OpenSearch/blob/632eb44a541b28ef16ed261904b45d74b84f3b9f/server/src/main/java/org/opensearch/node/Node.java#L1117">Guice Bindings</a>
 * These bindings are later used to inject DataSourceService in TransportActions.
 * Since the instances are not bound to interface,
 * it is leading to construction errors when we have DataSourceService as parameter in Constructor.
 * Inorder to circumvent this problem,
 * we have introduced this DataSourceServiceHolder class which is a concrete class.
 * This class is bound in guice injector and later injected to TransportAction classes.
 */
@Data
@AllArgsConstructor
public class DataSourceServiceHolder {
  private DataSourceService dataSourceService;

}
