/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/** Stores Spark parameter composers and dispatch compose request to each composer */
public class SparkParameterComposerCollection {
  Collection<GeneralSparkParameterComposer> generalComposers = new ArrayList<>();
  Map<DataSourceType, Collection<DataSourceSparkParameterComposer>> datasourceComposers =
      new HashMap<>();

  public void register(DataSourceType dataSourceType, DataSourceSparkParameterComposer composer) {
    if (!datasourceComposers.containsKey(dataSourceType)) {
      datasourceComposers.put(dataSourceType, new LinkedList<>());
    }
    datasourceComposers.get(dataSourceType).add(composer);
  }

  public void register(GeneralSparkParameterComposer composer) {
    generalComposers.add(composer);
  }

  /** Execute composers associated with the datasource type */
  public void composeByDataSource(
      DataSourceMetadata dataSourceMetadata,
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    for (DataSourceSparkParameterComposer composer :
        getComposersFor(dataSourceMetadata.getConnector())) {
      composer.compose(dataSourceMetadata, sparkSubmitParameters, dispatchQueryRequest, context);
    }
  }

  /** Execute all the registered generic composers */
  public void compose(
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    for (GeneralSparkParameterComposer composer : generalComposers) {
      composer.compose(sparkSubmitParameters, dispatchQueryRequest, context);
    }
  }

  private Collection<DataSourceSparkParameterComposer> getComposersFor(DataSourceType type) {
    return datasourceComposers.getOrDefault(type, ImmutableList.of());
  }

  public boolean isComposerRegistered(DataSourceType type) {
    return datasourceComposers.containsKey(type);
  }
}
