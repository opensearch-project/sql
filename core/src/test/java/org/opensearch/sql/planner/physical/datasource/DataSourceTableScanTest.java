/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
public class DataSourceTableScanTest {

  @Mock private DataSourceService dataSourceService;

  @Mock private StorageEngine storageEngine;

  private DataSourceTableScan dataSourceTableScan;

  @BeforeEach
  private void setUp() {
    dataSourceTableScan = new DataSourceTableScan(dataSourceService);
  }

  @Test
  void testExplain() {
    assertEquals("GetDataSourcesInfoRequest{}", dataSourceTableScan.explain());
  }

  @Test
  void testIterator() {
    Set<DataSource> dataSourceSet = new HashSet<>();
    dataSourceSet.add(new DataSource("prometheus", DataSourceType.PROMETHEUS, storageEngine));
    dataSourceSet.add(new DataSource("opensearch", DataSourceType.OPENSEARCH, storageEngine));
    Set<DataSourceMetadata> dataSourceMetadata =
        dataSourceSet.stream()
            .map(
                dataSource ->
                    new DataSourceMetadata(
                        dataSource.getName(),
                        dataSource.getConnectorType(),
                        Collections.emptyList(),
                        ImmutableMap.of()))
            .collect(Collectors.toSet());
    when(dataSourceService.getDataSourceMetadata(true)).thenReturn(dataSourceMetadata);

    assertFalse(dataSourceTableScan.hasNext());
    dataSourceTableScan.open();
    assertTrue(dataSourceTableScan.hasNext());
    Set<ExprValue> exprTupleValues = new HashSet<>();
    while (dataSourceTableScan.hasNext()) {
      exprTupleValues.add(dataSourceTableScan.next());
    }

    Set<ExprValue> expectedExprTupleValues = new HashSet<>();
    for (DataSource dataSource : dataSourceSet) {
      expectedExprTupleValues.add(
          new ExprTupleValue(
              new LinkedHashMap<>(
                  ImmutableMap.of(
                      "DATASOURCE_NAME", ExprValueUtils.stringValue(dataSource.getName()),
                      "CONNECTOR_TYPE",
                          ExprValueUtils.stringValue(dataSource.getConnectorType().name())))));
    }
    assertEquals(expectedExprTupleValues, exprTupleValues);
  }
}
