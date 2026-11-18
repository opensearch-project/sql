/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the cost of materializing a Calcite JDBC {@link ResultSet} into {@code Map<String,
 * ExprValue>} rows under a wide schema. Reading each cell by column name — the pre-fix path in
 * {@code OpenSearchExecutionEngine.buildResultSet} — triggers Avatica's case-insensitive linear
 * scan of the column metadata on every call and makes materialization O(rows × columns²). Reading
 * by JDBC ordinal — the current path — is linear.
 *
 * <p>Run with: {@code ./gradlew :benchmarks:jmh -Pjmh.includes=BuildResultSetBenchmark}. The two
 * {@code @Benchmark} methods below mirror the two code paths; the ratio between them should grow
 * with the column count.
 */
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
public class BuildResultSetBenchmark {

  @Param({"100", "1000", "5000"})
  private int columns;

  @Param({"100"})
  private int rows;

  private Connection connection;
  private String selectAll;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    Properties info = new Properties();
    // Match the engine's default (case-sensitive=false) so getObject(String) hits the same
    // equalsIgnoreCase scan it does at runtime.
    info.setProperty("caseSensitive", "false");
    connection = DriverManager.getConnection("jdbc:calcite:", info);
    SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
    SchemaPlus bench = root.add("BENCH", new AbstractSchema());
    bench.add("T", new WideTable(columns, rows));
    StringBuilder sb = new StringBuilder("SELECT ");
    for (int i = 1; i <= columns; i++) {
      if (i > 1) sb.append(',');
      sb.append("\"c").append(i).append('"');
    }
    sb.append(" FROM \"BENCH\".\"T\"");
    selectAll = sb.toString();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (connection != null) connection.close();
  }

  /** Pre-fix behaviour: read each cell by column name. */
  @Benchmark
  public void materializeByName(Blackhole bh) throws Exception {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectAll)) {
      ResultSetMetaData md = rs.getMetaData();
      int cc = md.getColumnCount();
      List<Map<String, Object>> all = new ArrayList<>(rows);
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>(cc);
        for (int i = 1; i <= cc; i++) {
          String name = md.getColumnName(i);
          row.put(name, rs.getObject(name));
        }
        all.add(row);
      }
      bh.consume(all);
    }
  }

  /** Current behaviour: read each cell by JDBC ordinal, matching the fix for issue #5939. */
  @Benchmark
  public void materializeByOrdinal(Blackhole bh) throws Exception {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectAll)) {
      ResultSetMetaData md = rs.getMetaData();
      int cc = md.getColumnCount();
      String[] names = new String[cc];
      for (int i = 1; i <= cc; i++) {
        names[i - 1] = md.getColumnName(i);
      }
      List<Map<String, Object>> all = new ArrayList<>(rows);
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>(cc);
        for (int i = 1; i <= cc; i++) {
          row.put(names[i - 1], rs.getObject(i));
        }
        all.add(row);
      }
      bh.consume(all);
    }
  }

  /** In-memory table with {@code columns} VARCHAR fields and {@code rows} identical string rows. */
  static class WideTable implements ScannableTable {
    private final int columns;
    private final int rows;

    WideTable(int columns, int rows) {
      this.columns = columns;
      this.rows = rows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      RelDataTypeFactory.Builder builder = factory.builder();
      for (int i = 1; i <= columns; i++) {
        builder.add("c" + i, SqlTypeName.VARCHAR);
      }
      return builder.build();
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      Object[][] data = new Object[rows][columns];
      for (int r = 0; r < rows; r++) {
        for (int c = 0; c < columns; c++) {
          data[r][c] = "v";
        }
      }
      return Linq4j.asEnumerable(data);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.of((double) rows, ImmutableList.of(), RelCollations.createSingleton(0));
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
