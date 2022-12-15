/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.After;
import org.junit.Test;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.executor.DefaultExecutionEngine;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.StreamingQueryPlan;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.filesystem.storage.FSDataSourceFactory;
import org.opensearch.sql.filesystem.storage.FSStorageEngine;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.storage.DataSourceFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@ThreadLeakFilters(filters = {StreamingQueryIT.HadoopFSThreadsFilter.class})
public class StreamingQueryIT extends PPLIntegTestCase {

  private static final int INTERVAL_IN_SECONDS = 1;

  private static final String DATASOURCE_NAME = "fs";

  private static final String TABLE_NAME = "mock";

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
  public void clean() throws InterruptedException, IOException {
    query.close();
    source.close();
  }

  StreamingQuery fromFile(java.nio.file.Path path) {
    return new StreamingQuery(path);
  }

  class Source {
    @Getter private final java.nio.file.Path tempDir;

    public Source() throws IOException {
      tempDir = Files.createTempDirectory("tempDir");
    }

    Source add(int v) throws IOException {
      java.nio.file.Path path = Files.createFile(tempDir.resolve(UUID.randomUUID().toString()));
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

    final QueryId queryId = QueryId.queryId();

    final FSStorageEngine storageEngine;

    public StreamingQuery(java.nio.file.Path tempDir) {
      result.set(0);
      context = new AnnotationConfigApplicationContext();
      context.refresh();
      DataSourceService dataSourceService =
          new DataSourceServiceImpl(
              new ImmutableSet.Builder<DataSourceFactory>()
                  .add(new FSDataSourceFactory(tempDir.toUri(), result))
                  .build());
      dataSourceService.addDataSource(fsDataSourceMetadata());
      context.registerBean(DataSourceService.class, () -> dataSourceService);
      storageEngine =
          (FSStorageEngine) dataSourceService.getDataSource(DATASOURCE_NAME).getStorageEngine();
      final BuiltinFunctionRepository functionRepository = BuiltinFunctionRepository.getInstance();
      Analyzer analyzer =
          new Analyzer(
              new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
      Planner planner = new Planner(LogicalPlanOptimizer.create());

      queryManager = DefaultQueryManager.defaultQueryManager();
      queryService = new QueryService(analyzer, new DefaultExecutionEngine(), planner);
    }

    public StreamingQuery run() {
      queryManager.submit(
          new StreamingQueryPlan(
              queryId,
              AstDSL.relation(AstDSL.qualifiedName(DATASOURCE_NAME, "default", TABLE_NAME)),
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

      storageEngine.close();
      context.close();
      queryManager.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner could not close.
   * https://www.mail-archive.com/common-issues@hadoop.apache.org/msg232722.html
   */
  public static class HadoopFSThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      return t.getName().contains("StatisticsDataReferenceCleaner");
    }
  }

  private DataSourceMetadata fsDataSourceMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(DATASOURCE_NAME);
    dataSourceMetadata.setConnector(DataSourceType.FILESYSTEM);
    dataSourceMetadata.setProperties(ImmutableMap.of());
    return dataSourceMetadata;
  }
}
