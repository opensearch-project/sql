package org.opensearch.sql.opensearch.expression;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.parse.GrokExpression;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.parse.RegexExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.expression.window.ranking.RankingWindowFunction;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.config.TestConfig;
import org.opensearch.sql.opensearch.functions.OpenSearchFunctions;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

public class OpenSearchDSL {
  private OpenSearchDSL() {}
    public static LiteralExpression literal(Byte value) {
      return new LiteralExpression(ExprValueUtils.byteValue(value));
    }

    public static LiteralExpression literal(Short value) {
      return new LiteralExpression(new ExprShortValue(value));
    }

    public static LiteralExpression literal(Integer value) {
      return new LiteralExpression(ExprValueUtils.integerValue(value));
    }

    public static LiteralExpression literal(Long value) {
      return new LiteralExpression(ExprValueUtils.longValue(value));
    }

    public static LiteralExpression literal(Float value) {
      return new LiteralExpression(ExprValueUtils.floatValue(value));
    }

    public static LiteralExpression literal(Double value) {
      return new LiteralExpression(ExprValueUtils.doubleValue(value));
    }

    public static LiteralExpression literal(String value) {
      return new LiteralExpression(ExprValueUtils.stringValue(value));
    }

    public static LiteralExpression literal(Boolean value) {
      return new LiteralExpression(ExprValueUtils.booleanValue(value));
    }

    public static LiteralExpression literal(ExprValue value) {
      return new LiteralExpression(value);
    }

    /** Wrap a number to {@link LiteralExpression}. */
    public static LiteralExpression literal(Number value) {
      if (value instanceof Integer) {
        return new LiteralExpression(ExprValueUtils.integerValue(value.intValue()));
      } else if (value instanceof Long) {
        return new LiteralExpression(ExprValueUtils.longValue(value.longValue()));
      } else if (value instanceof Float) {
        return new LiteralExpression(ExprValueUtils.floatValue(value.floatValue()));
      } else {
        return new LiteralExpression(ExprValueUtils.doubleValue(value.doubleValue()));
      }
    }

    public static ReferenceExpression ref(String ref, ExprType type) {
      return new ReferenceExpression(ref, type);
    }

    /**
     * Wrap a named expression if not yet. The intent is that different languages may use Alias or not
     * when building AST. This caused either named or unnamed expression is resolved by analyzer. To
     * make unnamed expression acceptable for logical project, it is required to wrap it by named
     * expression here before passing to logical project.
     *
     * @param expression expression
     * @return expression if named already or expression wrapped by named expression.
     */
    public static NamedExpression named(Expression expression) {
      if (expression instanceof NamedExpression) {
        return (NamedExpression) expression;
      }
      if (expression instanceof ParseExpression) {
        return named(
            ((ParseExpression) expression).getIdentifier().valueOf().stringValue(), expression);
      }
      return named(expression.toString(), expression);
    }

    public static NamedExpression named(String name, Expression expression) {
      return new NamedExpression(name, expression);
    }

    public static NamedExpression named(String name, Expression expression, String alias) {
      return new NamedExpression(name, expression, alias);
    }

    public static NamedAggregator named(String name, Aggregator aggregator) {
      return new NamedAggregator(name, aggregator);
    }

    public static NamedArgumentExpression namedArgument(String argName, Expression value) {
      return new NamedArgumentExpression(argName, value);
    }

    public static NamedArgumentExpression namedArgument(String name, String value) {
      return namedArgument(name, literal(value));
    }

    public static FunctionExpression nested(Expression... expressions) {
      return compile(FunctionProperties.None, BuiltinFunctionName.NESTED, expressions);
    }

    public static FunctionExpression match(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.MATCH, args);
    }

    public static FunctionExpression match_phrase(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_PHRASE, args);
    }

    public static FunctionExpression match_phrase_prefix(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_PHRASE_PREFIX, args);
    }

    public static FunctionExpression multi_match(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.MULTI_MATCH, args);
    }

    public static FunctionExpression simple_query_string(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.SIMPLE_QUERY_STRING, args);
    }

    public static FunctionExpression query(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.QUERY, args);
    }

    public static FunctionExpression query_string(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.QUERY_STRING, args);
    }

    public static FunctionExpression match_bool_prefix(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.MATCH_BOOL_PREFIX, args);
    }

    public static FunctionExpression wildcard_query(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.WILDCARD_QUERY, args);
    }

    public static FunctionExpression score(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.SCORE, args);
    }

    public static FunctionExpression scorequery(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.SCOREQUERY, args);
    }

    public static FunctionExpression score_query(Expression... args) {
      return compile(FunctionProperties.None, BuiltinFunctionName.SCORE_QUERY, args);
    }

  // Make this add OpenSearchStorageEngine
  @ExtendWith(MockitoExtension.class)
  protected static StorageEngine storageEngine() {
    return new StorageEngine() {
      @Getter
      @Mock
      private OpenSearchClient client;
      @Getter
      @Mock
      private Settings settings;

      @Override
      public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
        return table();
      }

      @Override
      public Collection<FunctionResolver> getFunctions() {
        return OpenSearchFunctions.getResolvers();
      }
    };
  }

  protected static Table table() {
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
        return TestConfig.typeMapping;
      }

      @Override
      public PhysicalPlan implement(LogicalPlan plan) {
        throw new UnsupportedOperationException();
      }

      public Map<String, ExprType> getReservedFieldTypes() {
        return ImmutableMap.of("_test", STRING);
      }
    };
  }


  private static class DefaultDataSourceService implements DataSourceService {

    private final DataSource opensearchDataSource = new DataSource(DEFAULT_DATASOURCE_NAME,
        DataSourceType.OPENSEARCH, storageEngine());


    @Override
    public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
      return Stream.of(opensearchDataSource)
          .map(ds -> new DataSourceMetadata(ds.getName(),
              ds.getConnectorType(), Collections.emptyList(),
              ImmutableMap.of())).collect(Collectors.toSet());
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
      return opensearchDataSource;
    }

    @Override
    public void updateDataSource(DataSourceMetadata dataSourceMetadata) {

    }

    @Override
    public void deleteDataSource(String dataSourceName) {
    }

    @Override
    public Boolean dataSourceExists(String dataSourceName) {
      return dataSourceName.equals(DEFAULT_DATASOURCE_NAME);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends FunctionImplementation> T compile(
      FunctionProperties functionProperties, BuiltinFunctionName bfn, Expression... args) {
    return (T)
        BuiltinFunctionRepository.getInstance(new DefaultDataSourceService())
            .compile(functionProperties, bfn.getName(), Arrays.asList(args));
  }
}
