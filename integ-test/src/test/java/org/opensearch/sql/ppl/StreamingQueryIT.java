/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.opensearch.sql.CatalogSchemaName;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.StreamingQueryPlan;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;
import org.opensearch.sql.filesystem.streaming.FileSystemStreamSource;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.plugin.catalog.CatalogServiceImpl;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.split.Split;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@ThreadLeakFilters(filters = {StreamingQueryIT.HadoopFSThreadsFilter.class})
public class StreamingQueryIT extends PPLIntegTestCase {

  private static final int INTERVAL_IN_SECONDS = 1;

  private final AtomicInteger result = new AtomicInteger(0);

  private Source source;

  private StreamingQuery query;

  @Test
  public void testStreamingQuery() throws IOException, InterruptedException {
    source = new Source();
    query = fromFile(source.tempDir);
    query.run();

    source.add(0);
    source.add(1);
    query.sumShouldBe(1);

    // no source update.
    query.sumShouldBe(1);

    source.add(1);
    query.sumShouldBe(2);
  }

  @After
  void clean() throws InterruptedException, IOException {
    query.close();
    source.close();
  }

  StreamingQuery fromFile(java.nio.file.Path path) throws IOException {
    return new StreamingQuery(path);
  }

  class Source {
    @Getter
    private final java.nio.file.Path tempDir;

    public Source() throws IOException {
      tempDir = Files.createTempDirectory("tempDir");
    }

    Source add(int v) throws IOException {
      java.nio.file.Path path =
          Files.createFile(tempDir.resolve(UUID.randomUUID().toString()));
      String buf = String.valueOf(v);
      FileOutputStream outputStream = new FileOutputStream(path.toFile());
      outputStream.write(buf.getBytes(StandardCharsets.UTF_8));
      outputStream.close();
      return this;
    }

    void close() throws IOException {
      Files.walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
      assertFalse("temp dir still exist", Files.exists(tempDir));
    }
  }

  class StreamingQuery {

    final AnnotationConfigApplicationContext context;

    final DefaultQueryManager queryManager;

    final QueryService queryService;

    final FileSystem fs;

    final QueryId queryId = QueryId.queryId();

    public StreamingQuery(java.nio.file.Path tempDir) throws IOException {
      result.set(0);
      fs = FileSystem.get(new Configuration());
      context = new AnnotationConfigApplicationContext();
      context.register(ExpressionConfig.class);
      context.refresh();
      BuiltinFunctionRepository functionRepository =
          context.getBean(BuiltinFunctionRepository.class);
      CatalogService catalogService = CatalogServiceImpl.getInstance();
      CatalogServiceImpl.getInstance()
          .registerDefaultOpenSearchCatalog(new FSStorageEngine(fs, new Path(tempDir.toUri())));
      Analyzer analyzer =
          new Analyzer(
              new ExpressionAnalyzer(functionRepository), catalogService, functionRepository);
      Planner planner = new Planner(LogicalPlanOptimizer.create(new DSL(functionRepository)));

      queryManager = DefaultQueryManager.defaultQueryManager();
      queryService = new QueryService(analyzer, new DefaultExecutionEngine(), planner);
    }

    public StreamingQuery run() {
      queryManager.submit(
          new StreamingQueryPlan(
              queryId,
              AstDSL.relation("mock"),
              queryService,
              new ResponseListener<>() {
                @Override
                public void onResponse(ExecutionEngine.QueryResponse response) {
                  fail();
                }

                @Override
                public void onFailure(Exception e) {
                  fail();
                }
              },
              new StreamingQueryPlan.IntervalTriggerExecution(1)));
      return this;
    }

    void sumShouldBe(int expected) throws InterruptedException {
      TimeUnit.SECONDS.sleep(INTERVAL_IN_SECONDS);
      assertEquals(expected, result.get());
    }

    void close() throws InterruptedException, IOException {
      assertTrue(queryManager.cancel(queryId));

      fs.close();
      context.close();
      queryManager.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /** FileSystem StorageEngine. */
  @RequiredArgsConstructor
  class FSStorageEngine implements StorageEngine {

    private final FileSystem fs;

    private final Path basePath;

    @Override
    public Table getTable(CatalogSchemaName catalogSchemaName, String tableName) {
      return new FSTable(fs, basePath);
    }
  }

  /** FileSystem Table. */
  @RequiredArgsConstructor
  class FSTable implements Table {
    private final FileSystem fs;

    private final Path basePath;

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
      fail();
      return "";
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

  class DefaultExecutionEngine implements ExecutionEngine {
    @Override
    public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
      execute(plan, ExecutionContext.emptyExecutionContext(), listener);
    }

    @Override
    public void execute(PhysicalPlan plan, ExecutionContext context,
                        ResponseListener<QueryResponse> listener) {
      try {
        List<ExprValue> result = new ArrayList<>();

        context.getSplit().ifPresent(plan::add);
        plan.open();

        while (plan.hasNext()) {
          result.add(plan.next());
        }
        QueryResponse response =
            new QueryResponse(new Schema(new ArrayList<>()), new ArrayList<>());
        listener.onResponse(response);
      } catch (Exception e) {
        listener.onFailure(e);
      } finally {
        plan.close();
      }
    }

    @Override
    public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
      fail();
    }
  }

  /**
   * org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner could not close.
   * https://www.mail-archive.com/common-issues@hadoop.apache.org/msg232722.html
   */
  static public class HadoopFSThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      return t.getName().contains("StatisticsDataReferenceCleaner");
    }
  }
}
