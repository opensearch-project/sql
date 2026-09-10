/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.calcite.plan.AbstractOpenSearchTable;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.executor.TimeBounds;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.SupportsIndexPruning;

/** How {@link OpenSearchSchema} hands the request's time bounds to the tables it resolves. */
@ExtendWith(MockitoExtension.class)
class OpenSearchSchemaTest {

  private static final TimeBounds BOUNDS = new TimeBounds("ts", "now-15m", "now");

  @Mock private DataSourceService dataSourceService;
  @Mock private DataSource dataSource;

  /** Records what each resolution was asked for. */
  private final List<TimeBounds> resolvedWith = new ArrayList<>();

  @BeforeEach
  void setUp() {
    when(dataSourceService.getDataSource(any())).thenReturn(dataSource);
  }

  /**
   * The bounds are a statement about the window the caller is asking about, so they hold for every
   * source the query reads -- a subsearch's as much as the primary one.
   */
  @Test
  void shouldNarrowEveryTableItResolves() {
    OpenSearchSchema schema = givenPruningEngine(BOUNDS);

    schema.getTableMap().get("primary");
    schema.getTableMap().get("subsearch");

    assertEquals(List.of(BOUNDS, BOUNDS), resolvedWith);
  }

  @Test
  void shouldResolveWithoutBoundsWhenTheRequestDeclaredNone() {
    OpenSearchSchema schema = givenPruningEngine(null);

    schema.getTableMap().get("primary");

    assertEquals(List.of(), resolvedWith);
  }

  /** A cached table is not re-resolved, so the probe runs once per distinct name. */
  @Test
  void shouldResolveARepeatedNameOnce() {
    OpenSearchSchema schema = givenPruningEngine(BOUNDS);

    Table first = schema.getTableMap().get("primary");
    Table again = schema.getTableMap().get("primary");

    assertSame(first, again);
    assertEquals(List.of(BOUNDS), resolvedWith);
  }

  /** Most engines cannot prune, and must not be asked to. */
  @Test
  void shouldFallBackForAnEngineThatCannotPrune() {
    StorageEngine plain = mock(StorageEngine.class);
    when(dataSource.getStorageEngine()).thenReturn(plain);
    when(plain.getTable(any(), any())).thenReturn(mock(AbstractOpenSearchTable.class));
    OpenSearchSchema schema = new OpenSearchSchema(dataSourceService, BOUNDS);

    schema.getTableMap().get("primary");

    verify(plain).getTable(any(DataSourceSchemaName.class), any(String.class));
  }

  private OpenSearchSchema givenPruningEngine(TimeBounds bounds) {
    PruningEngine engine = new PruningEngine();
    when(dataSource.getStorageEngine()).thenReturn(engine);
    return new OpenSearchSchema(dataSourceService, bounds);
  }

  /** A {@link StorageEngine} that can prune, recording what each resolution asked for. */
  private final class PruningEngine implements StorageEngine, SupportsIndexPruning {

    @Override
    public org.opensearch.sql.storage.Table getTable(
        DataSourceSchemaName dataSourceSchemaName, String tableName) {
      return mock(AbstractOpenSearchTable.class);
    }

    @Override
    public org.opensearch.sql.storage.Table getTable(
        DataSourceSchemaName dataSourceSchemaName, String tableName, TimeBounds bounds) {
      resolvedWith.add(bounds);
      return mock(AbstractOpenSearchTable.class);
    }
  }

  /** Never asked for: pruning replaces the unbounded resolution rather than following it. */
  @Test
  void shouldNotAlsoResolveUnbounded() {
    StorageEngine engine = mock(PruningEngine.class);
    when(dataSource.getStorageEngine()).thenReturn(engine);
    when(((SupportsIndexPruning) engine).getTable(any(), any(), any()))
        .thenReturn(mock(AbstractOpenSearchTable.class));

    new OpenSearchSchema(dataSourceService, BOUNDS).getTableMap().get("primary");

    verify(engine, never()).getTable(any(DataSourceSchemaName.class), any(String.class));
  }
}
