/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.config.TestConfig;
import org.opensearch.sql.data.type.ExprType;
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
    return (catalogSchemaName, tableName) -> table;
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
  protected Table catalogTable() {
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
  protected CatalogService catalogService() {
    return new DefaultCatalogService();
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
  protected CatalogService catalogService;

  @Autowired
  protected Environment<Expression, ExprType> typeEnv;

  @Bean
  protected Analyzer analyzer(ExpressionAnalyzer expressionAnalyzer,
                              CatalogService catalogService,
                              StorageEngine storageEngine,
                              Table table) {
    catalogService.registerDefaultOpenSearchCatalog(storageEngine);
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
    return new Analyzer(expressionAnalyzer, catalogService, functionRepository);
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

  private class DefaultCatalogService implements CatalogService {

    private StorageEngine storageEngine = storageEngine();
    private final Catalog catalog
        = new Catalog("prometheus", ConnectorType.PROMETHEUS, storageEngine);


    @Override
    public Set<Catalog> getCatalogs() {
      return ImmutableSet.of(catalog);
    }

    @Override
    public Catalog getCatalog(String catalogName) {
      return catalog;
    }

    @Override
    public void registerDefaultOpenSearchCatalog(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
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
