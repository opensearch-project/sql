/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.config.TestConfig;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.RequestContext;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

public class AnalyzerTestBase {

  protected Map<String, ExprType> typeMapping() {
    return TestConfig.typeMapping;
  }

  protected StorageEngine storageEngine() {
    return (dataSourceSchemaName, tableName) -> table;
  }

  protected StorageEngine prometheusStorageEngine() {
    return new StorageEngine() {
      @Override
      public Collection<FunctionResolver> getFunctions() {
        return Collections.singletonList(
            new FunctionResolver() {

              @Override
              public Pair<FunctionSignature, FunctionBuilder> resolve(
                  FunctionSignature unresolvedSignature) {
                FunctionName functionName = FunctionName.of("query_range");
                FunctionSignature functionSignature =
                    new FunctionSignature(functionName, List.of(STRING, LONG, LONG, STRING));
                return Pair.of(
                    functionSignature,
                    (functionProperties, args) ->
                        new TestTableFunctionImplementation(functionName, args, table));
              }

              @Override
              public FunctionName getFunctionName() {
                return FunctionName.of("query_range");
              }
            });
      }

      @Override
      public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
        return table;
      }
    };
  }

  protected Table table() {
    return Optional.ofNullable(table)
        .orElseGet(
            () ->
                new Table() {
                  @Override
                  public boolean exists() {
                    return true;
                  }

                  @Override
                  public void create(Map<String, ExprType> schema) {
                    throw new UnsupportedOperationException("Create table is not supported");
                  }

                  @Override
                  public Map<String, ExprType> getFieldTypes() {
                    return typeMapping();
                  }

                  @Override
                  public PhysicalPlan implement(LogicalPlan plan) {
                    throw new UnsupportedOperationException();
                  }

                  public Map<String, ExprType> getReservedFieldTypes() {
                    return ImmutableMap.of("_test", STRING);
                  }
                });
  }

  protected DataSourceService dataSourceService() {
    return Optional.ofNullable(dataSourceService).orElseGet(DefaultDataSourceService::new);
  }

  protected SymbolTable symbolTable() {
    SymbolTable symbolTable = new SymbolTable();
    typeMapping()
        .entrySet()
        .forEach(
            entry ->
                symbolTable.store(
                    new Symbol(Namespace.FIELD_NAME, entry.getKey()), entry.getValue()));
    return symbolTable;
  }

  protected Environment<Expression, ExprType> typeEnv() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        ReferenceExpression refExpr = (ReferenceExpression) var;
        if (typeMapping().containsKey(refExpr.getAttr())) {
          return typeMapping().get(refExpr.getAttr());
        }
      }
      throw new ExpressionEvaluationException("type resolved failed");
    };
  }

  protected final AnalysisContext analysisContext = analysisContext(typeEnvironment(symbolTable()));

  protected final ExpressionAnalyzer expressionAnalyzer = expressionAnalyzer();

  protected final Table table = table();

  protected final DataSourceService dataSourceService = dataSourceService();

  protected final Analyzer analyzer = analyzer(expressionAnalyzer(), dataSourceService);

  protected Analyzer analyzer(
      ExpressionAnalyzer expressionAnalyzer, DataSourceService dataSourceService) {
    BuiltinFunctionRepository functionRepository = BuiltinFunctionRepository.getInstance();
    return new Analyzer(expressionAnalyzer, dataSourceService, functionRepository);
  }

  protected TypeEnvironment typeEnvironment(SymbolTable symbolTable) {
    return new TypeEnvironment(null, symbolTable);
  }

  protected AnalysisContext analysisContext(TypeEnvironment typeEnvironment) {
    return new AnalysisContext(typeEnvironment);
  }

  protected ExpressionAnalyzer expressionAnalyzer() {
    return new ExpressionAnalyzer(BuiltinFunctionRepository.getInstance());
  }

  protected void assertAnalyzeEqual(LogicalPlan expected, UnresolvedPlan unresolvedPlan) {
    LogicalPlan actual = analyze(unresolvedPlan);
    assertEquals(expected, actual);
  }

  protected LogicalPlan analyze(UnresolvedPlan unresolvedPlan) {
    return analyzer.analyze(unresolvedPlan, analysisContext);
  }

  private class DefaultDataSourceService implements DataSourceService {

    private final DataSource opensearchDataSource =
        new DataSource(DEFAULT_DATASOURCE_NAME, DataSourceType.OPENSEARCH, storageEngine());
    private final DataSource prometheusDataSource =
        new DataSource("prometheus", DataSourceType.PROMETHEUS, prometheusStorageEngine());

    @Override
    public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
      return Stream.of(opensearchDataSource, prometheusDataSource)
          .map(
              ds ->
                  new DataSourceMetadata.Builder()
                      .setName(ds.getName())
                      .setConnector(ds.getConnectorType())
                      .build())
          .collect(Collectors.toSet());
    }

    @Override
    public DataSourceMetadata getDataSourceMetadata(String name) {
      return null;
    }

    @Override
    public void createDataSource(DataSourceMetadata metadata) {
      throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public DataSource getDataSource(String dataSourceName) {
      if ("prometheus".equals(dataSourceName)) {
        return prometheusDataSource;
      } else {
        return opensearchDataSource;
      }
    }

    @Override
    public void updateDataSource(DataSourceMetadata dataSourceMetadata) {}

    @Override
    public void patchDataSource(Map<String, Object> dataSourceData) {}

    @Override
    public void deleteDataSource(String dataSourceName) {}

    @Override
    public Boolean dataSourceExists(String dataSourceName) {
      return dataSourceName.equals(DEFAULT_DATASOURCE_NAME) || dataSourceName.equals("prometheus");
    }

    @Override
    public DataSourceMetadata verifyDataSourceAccessAndGetRawMetadata(
        String dataSourceName, RequestContext requestContext) {
      return null;
    }
  }

  private class TestTableFunctionImplementation implements TableFunctionImplementation {

    private final FunctionName functionName;

    private final List<Expression> arguments;

    private final Table table;

    public TestTableFunctionImplementation(
        FunctionName functionName, List<Expression> arguments, Table table) {
      this.functionName = functionName;
      this.arguments = arguments;
      this.table = table;
    }

    @Override
    public FunctionName getFunctionName() {
      return functionName;
    }

    @Override
    public List<Expression> getArguments() {
      return this.arguments;
    }

    @Override
    public Table applyArguments() {
      return table;
    }
  }
}
