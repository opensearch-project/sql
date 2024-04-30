package org.opensearch.sql.spark.flint;

import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;

public interface IndexDMLResultStorageService {

  IndexDMLResult createIndexDMLResult(IndexDMLResult result, String datasourceName);
}
