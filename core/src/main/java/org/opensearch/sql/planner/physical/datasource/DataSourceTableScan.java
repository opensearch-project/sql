/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * This class handles table scan of data source table. Right now these are derived from
 * dataSourceService thorough static fields. In future this might scan data from underlying
 * datastore if we start persisting datasource info somewhere.
 */
public class DataSourceTableScan extends TableScanOperator {

  private final DataSourceService dataSourceService;

  private Iterator<ExprValue> iterator;

  public DataSourceTableScan(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
    this.iterator = Collections.emptyIterator();
  }

  @Override
  public String explain() {
    return "GetDataSourcesInfoRequest{}";
  }

  @Override
  public void open() {
    List<ExprValue> exprValues = new ArrayList<>();
    Set<DataSourceMetadata> dataSourceMetadataSet = dataSourceService.getDataSourceMetadata(true);
    for (DataSourceMetadata dataSourceMetadata : dataSourceMetadataSet) {
      exprValues.add(
          new ExprTupleValue(
              new LinkedHashMap<>(
                  ImmutableMap.of(
                      "DATASOURCE_NAME",
                      ExprValueUtils.stringValue(dataSourceMetadata.getName()),
                      "CONNECTOR_TYPE",
                      ExprValueUtils.stringValue(dataSourceMetadata.getConnector().name())))));
    }
    iterator = exprValues.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }
}
