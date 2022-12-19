/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage;

import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;
import org.opensearch.sql.filesystem.streaming.FileSystemStreamSource;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.split.Split;

/**
 * FileSystem Table. Used for testing purpose.
 */
@RequiredArgsConstructor
public class FSTable implements Table {
  private final FileSystem fs;

  private final Path basePath;

  private final AtomicInteger result;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return ImmutableMap.of("value", ExprCoreType.INTEGER);
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return new Output(new FSScan(fs));
  }

  @Override
  public StreamingSource asStreamingSource() {
    return new FileSystemStreamSource(fs, basePath);
  }

  @RequiredArgsConstructor
  class Output extends PhysicalPlan {
    private final PhysicalPlan input;

    @Override
    public void open() {
      while (input.hasNext()) {
        result.addAndGet(input.next().integerValue());
      }
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public ExprValue next() {
      throw new UnsupportedOperationException();
    }

    // todo, need to refactor physical plan interface.
    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<PhysicalPlan> getChild() {
      return Collections.singletonList(input);
    }
  }

  @RequiredArgsConstructor
  class FSScan extends TableScanOperator {

    private final FileSystem fs;

    private Iterator<Path> paths;

    @Override
    public String explain() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      return paths.hasNext();
    }

    @SneakyThrows(IOException.class)
    @Override
    public ExprValue next() {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(paths.next())));
      // every file only contain one line.
      ExprValue result = ExprValueUtils.integerValue(Integer.valueOf(reader.readLine()));

      reader.close();
      return result;
    }

    @Override
    public void add(Split split) {
      paths = ((FileSystemSplit) split).getPaths().iterator();
    }
  }
}
