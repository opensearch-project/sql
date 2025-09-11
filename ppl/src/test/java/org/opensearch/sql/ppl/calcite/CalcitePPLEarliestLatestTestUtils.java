/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.sql.Date;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Shared test utilities for earliest/latest function tests. */
public class CalcitePPLEarliestLatestTestUtils {

  /**
   * Creates test data for LOGS table with @timestamp and created_at fields. Note: @timestamp and
   * created_at have different orderings to test explicit field usage.
   */
  public static ImmutableList<Object[]> createLogsTestData() {
    return ImmutableList.of(
        new Object[] {
          "server1",
          "ERROR",
          "Database connection failed",
          Date.valueOf("2023-01-01"),
          Date.valueOf("2023-01-05")
        },
        new Object[] {
          "server2",
          "INFO",
          "Service started",
          Date.valueOf("2023-01-02"),
          Date.valueOf("2023-01-04")
        },
        new Object[] {
          "server1",
          "WARN",
          "High memory usage",
          Date.valueOf("2023-01-03"),
          Date.valueOf("2023-01-03")
        },
        new Object[] {
          "server3",
          "ERROR",
          "Disk space low",
          Date.valueOf("2023-01-04"),
          Date.valueOf("2023-01-02")
        },
        new Object[] {
          "server2",
          "INFO",
          "Backup completed",
          Date.valueOf("2023-01-05"),
          Date.valueOf("2023-01-01")
        });
  }

  /** Custom table implementation with @timestamp field for earliest/latest testing. */
  @RequiredArgsConstructor
  public static class LogsTable implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("server", SqlTypeName.VARCHAR)
                .add("level", SqlTypeName.VARCHAR)
                .add("message", SqlTypeName.VARCHAR)
                .add("@timestamp", SqlTypeName.DATE)
                .add("created_at", SqlTypeName.DATE)
                .build();

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.of(0d, ImmutableList.of(), RelCollations.createSingleton(0));
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
}
