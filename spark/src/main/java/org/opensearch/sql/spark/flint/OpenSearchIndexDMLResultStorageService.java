package org.opensearch.sql.spark.flint;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;
import org.opensearch.sql.spark.execution.statestore.StateStore;

@RequiredArgsConstructor
public class OpenSearchIndexDMLResultStorageService implements IndexDMLResultStorageService {

  private final DataSourceService dataSourceService;
  private final StateStore stateStore;

  @Override
  public IndexDMLResult createIndexDMLResult(IndexDMLResult result, String datasourceName) {
    DataSourceMetadata dataSourceMetadata = dataSourceService.getDataSourceMetadata(datasourceName);
    return stateStore.create(result, IndexDMLResult::copy, dataSourceMetadata.getResultIndex());
  }
}
