/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.config.TestConfig;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
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
    return new StorageEngine() {
      @Override
      public Table getTable(String name) {
        return table;
      }
    };
  }

  @Bean
  protected Table table() {
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
  protected BuiltinFunctionRepository functionRepository;

  @Autowired
  protected DSL dsl;

  @Autowired
  protected AnalysisContext analysisContext;

  @Autowired
  protected ExpressionAnalyzer expressionAnalyzer;

  @Autowired
  protected Analyzer analyzer;

  @Autowired
  protected Table table;

  @Autowired
  protected Environment<Expression, ExprType> typeEnv;

  @Bean
  protected Analyzer analyzer(ExpressionAnalyzer expressionAnalyzer, CatalogService catalogService,
                              StorageEngine storageEngine) {
    catalogService.registerOpenSearchStorageEngine(storageEngine);
    return new Analyzer(expressionAnalyzer, catalogService);
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
  protected ExpressionAnalyzer expressionAnalyzer(DSL dsl, BuiltinFunctionRepository repo) {
    return new ExpressionAnalyzer(repo);
  }

  protected void assertAnalyzeEqual(LogicalPlan expected, UnresolvedPlan unresolvedPlan) {
    assertEquals(expected, analyze(unresolvedPlan));
  }

  protected LogicalPlan analyze(UnresolvedPlan unresolvedPlan) {
    return analyzer.analyze(unresolvedPlan, analysisContext);
  }

  private class DefaultCatalogService implements CatalogService {

    private StorageEngine storageEngine;

    @Override
    public StorageEngine getStorageEngine(String catalog) {
      return storageEngine;
    }

    @Override
    public Set<String> getCatalogs() {
      return ImmutableSet.of("prometheus");
    }

    @Override
    public void registerOpenSearchStorageEngine(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
    }

    @Override
    public void registerStorageEngine(String name, StorageEngine storageEngine) {
    }
  }
}
