/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.config.TestConfig;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;


public class AnalyzerTestBase {

  protected Map<String, ExprType> typeMapping() {
    return TestConfig.typeMapping;
  }

  @Bean
  protected StorageEngine storageEngine() {
    return (dataSourceSchemaName, tableName) -> table;
  }

  @Bean
  protected Table table() {
    return new Table() {
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
    };
  }

  @Bean
  protected Table dataSourceTable() {
    return new Table() {
      @Override
      public Map<String, ExprType> getFieldTypes() {
        return typeMapping();
      }

      @Override
      public PhysicalPlan implement(LogicalPlan plan) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Bean
  protected DataSourceService dataSourceService() {
    return new DefaultDataSourceService();
  }


  @Bean
  protected SymbolTable symbolTable() {
    SymbolTable symbolTable = new SymbolTable();
    typeMapping().entrySet()
        .forEach(
            entry -> symbolTable
                .store(new Symbol(Namespace.FIELD_NAME, entry.getKey()), entry.getValue()));
    return symbolTable;
  }

  @Bean
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

  @Autowired
  protected AnalysisContext analysisContext;

  @Autowired
  protected ExpressionAnalyzer expressionAnalyzer;

  @Autowired
  protected Analyzer analyzer;

  @Autowired
  protected Table table;

  @Autowired
  protected DataSourceService dataSourceService;

  @Autowired
  protected Environment<Expression, ExprType> typeEnv;

  @Bean
  protected Analyzer analyzer(ExpressionAnalyzer expressionAnalyzer,
                      DataSourceService dataSourceService,
                      Table table) {
    BuiltinFunctionRepository functionRepository = BuiltinFunctionRepository.getInstance();
    functionRepository.register("prometheus", new FunctionResolver() {

      @Override
      public Pair<FunctionSignature, FunctionBuilder> resolve(
          FunctionSignature unresolvedSignature) {
        FunctionName functionName = FunctionName.of("query_range");
        FunctionSignature functionSignature =
            new FunctionSignature(functionName, List.of(STRING, LONG, LONG, LONG));
        return Pair.of(functionSignature,
            (functionProperties, args) -> new TestTableFunctionImplementation(functionName, args,
                table));
      }

      @Override
      public FunctionName getFunctionName() {
        return FunctionName.of("query_range");
      }
    });
    return new Analyzer(expressionAnalyzer, dataSourceService, functionRepository);
  }

  @Bean
  protected TypeEnvironment typeEnvironment(SymbolTable symbolTable) {
    return new TypeEnvironment(null, symbolTable);
  }

  @Bean
  protected AnalysisContext analysisContext(TypeEnvironment typeEnvironment) {
    return new AnalysisContext(typeEnvironment);
  }

  @Bean
  protected ExpressionAnalyzer expressionAnalyzer() {
    return new ExpressionAnalyzer(BuiltinFunctionRepository.getInstance());
  }

  protected void assertAnalyzeEqual(LogicalPlan expected, UnresolvedPlan unresolvedPlan) {
    assertEquals(expected, analyze(unresolvedPlan));
  }

  protected LogicalPlan analyze(UnresolvedPlan unresolvedPlan) {
    return analyzer.analyze(unresolvedPlan, analysisContext);
  }

  private class DefaultDataSourceService implements DataSourceService {

    private StorageEngine storageEngine = storageEngine();
    private final DataSource dataSource
        = new DataSource("prometheus", DataSourceType.PROMETHEUS, storageEngine);


    @Override
    public Set<DataSourceMetadata> getDataSourceMetadataSet() {
      return ImmutableSet.of(new DataSourceMetadata(dataSource.getName(),
          dataSource.getConnectorType(), ImmutableMap.of()));
    }

    @Override
    public DataSource getDataSource(String dataSourceName) {
      return dataSource;
    }

    @Override
    public void createDataSource(DataSourceMetadata... metadatas) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDataSource(DataSourceMetadata dataSourceMetadata) {

    }

    @Override
    public void deleteDataSource(String dataSourceName) {

    }

    @Override
    public void bootstrapDataSources() {

    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private class TestTableFunctionImplementation implements TableFunctionImplementation {

    private FunctionName functionName;

    private List<Expression> arguments;

    private Table table;

    public TestTableFunctionImplementation(FunctionName functionName, List<Expression> arguments,
                                           Table table) {
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
