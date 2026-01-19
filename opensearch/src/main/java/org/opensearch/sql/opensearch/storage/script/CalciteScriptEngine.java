/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Calcite project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.opensearch.storage.script;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.DIGESTS;
import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.SOURCES;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.chrono.ChronoZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.LabelTarget;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.script.AggregationScript;
import org.opensearch.script.FieldScript;
import org.opensearch.script.FilterScript;
import org.opensearch.script.NumberSortScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptedMetricAggContexts;
import org.opensearch.script.StringSortScript;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.opensearch.storage.script.aggregation.CalciteAggregationScriptFactory;
import org.opensearch.sql.opensearch.storage.script.field.CalciteFieldScriptFactory;
import org.opensearch.sql.opensearch.storage.script.filter.CalciteFilterScriptFactory;
import org.opensearch.sql.opensearch.storage.script.scriptedmetric.CalciteScriptedMetricCombineScriptFactory;
import org.opensearch.sql.opensearch.storage.script.scriptedmetric.CalciteScriptedMetricInitScriptFactory;
import org.opensearch.sql.opensearch.storage.script.scriptedmetric.CalciteScriptedMetricMapScriptFactory;
import org.opensearch.sql.opensearch.storage.script.scriptedmetric.CalciteScriptedMetricReduceScriptFactory;
import org.opensearch.sql.opensearch.storage.script.sort.CalciteNumberSortScriptFactory;
import org.opensearch.sql.opensearch.storage.script.sort.CalciteStringSortScriptFactory;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;

/**
 * Custom expression script engine that supports using core engine expression code in DSL as a new
 * script language just like built-in Painless language.
 */
@RequiredArgsConstructor
public class CalciteScriptEngine implements ScriptEngine {

  private final RelJsonSerializer relJsonSerializer;

  public CalciteScriptEngine(RelOptCluster relOptCluster) {
    this.relJsonSerializer = new RelJsonSerializer(relOptCluster);
  }

  /** Expression script language name. */
  public static final String EXPRESSION_LANG_NAME = "opensearch_calcite_expression";

  /** All supported script contexts and function to create factory from expression. */
  private static final Map<
          ScriptContext<?>, BiFunction<Function1<DataContext, Object[]>, RelDataType, Object>>
      CONTEXTS =
          new ImmutableMap.Builder<
                  ScriptContext<?>,
                  BiFunction<Function1<DataContext, Object[]>, RelDataType, Object>>()
              .put(FilterScript.CONTEXT, CalciteFilterScriptFactory::new)
              .put(AggregationScript.CONTEXT, CalciteAggregationScriptFactory::new)
              .put(NumberSortScript.CONTEXT, CalciteNumberSortScriptFactory::new)
              .put(StringSortScript.CONTEXT, CalciteStringSortScriptFactory::new)
              .put(FieldScript.CONTEXT, CalciteFieldScriptFactory::new)
              .put(
                  ScriptedMetricAggContexts.InitScript.CONTEXT,
                  CalciteScriptedMetricInitScriptFactory::new)
              .put(
                  ScriptedMetricAggContexts.MapScript.CONTEXT,
                  CalciteScriptedMetricMapScriptFactory::new)
              .put(
                  ScriptedMetricAggContexts.CombineScript.CONTEXT,
                  CalciteScriptedMetricCombineScriptFactory::new)
              .put(
                  ScriptedMetricAggContexts.ReduceScript.CONTEXT,
                  CalciteScriptedMetricReduceScriptFactory::new)
              .build();

  @Override
  public String getType() {
    return EXPRESSION_LANG_NAME;
  }

  @Override
  public <T> T compile(
      String scriptName, String scriptCode, ScriptContext<T> context, Map<String, String> options) {
    RexNode rexNode = relJsonSerializer.deserialize(scriptCode);

    RexToLixTranslator.InputGetter getter =
        (blockBuilder, i, type) -> {
          throw new UnsupportedScriptException(
              "[BUG]There shouldn't be RexInputRef in the RexNode.");
        };
    String code =
        CalciteScriptEngine.translate(
            relJsonSerializer.getCluster().getRexBuilder(),
            List.of(rexNode),
            getter,
            new RelRecordType(List.of()));

    Function1<DataContext, Object[]> function =
        new RexExecutable(code, "generated Rex code").getFunction();

    if (CONTEXTS.containsKey(context)) {
      return context.factoryClazz.cast(CONTEXTS.get(context).apply(function, rexNode.getType()));
    }
    throw new IllegalStateException(
        String.format(
            "Script context is currently not supported: "
                + "all supported contexts [%s], given context [%s] ",
            CONTEXTS, context));
  }

  @Override
  public Set<ScriptContext<?>> getSupportedContexts() {
    return CONTEXTS.keySet();
  }

  public static final class UnsupportedScriptException extends RuntimeException {

    public UnsupportedScriptException(String message) {
      super(message);
    }

    public UnsupportedScriptException(Throwable cause) {
      super(cause);
    }
  }

  public static class ScriptDataContext implements DataContext {

    private final Map<String, ScriptDocValues<?>> docProvider;
    private final SourceLookup sourceLookup;
    private final long utcTimestamp;
    private final List<Source> sources;
    private final List<Object> digests;
    private final Map<String, Integer> parameterToIndex;

    public ScriptDataContext(
        Map<String, ScriptDocValues<?>> docProvider,
        SourceLookup sourceLookup,
        Map<String, Object> params,
        Map<String, Integer> parameterToIndex) {
      this.docProvider = docProvider;
      this.sourceLookup = sourceLookup;
      this.utcTimestamp = (long) params.get(Variable.UTC_TIMESTAMP.camelName);
      this.sources = ((List<Integer>) params.get(SOURCES)).stream().map(Source::fromValue).toList();
      this.digests = (List<Object>) params.get(DIGESTS);
      this.parameterToIndex = parameterToIndex;
    }

    @Override
    public @Nullable SchemaPlus getRootSchema() {
      return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
      return null;
    }

    @Override
    public Object get(String name) {
      // UTC_TIMESTAMP is a special variable used for some time related functions.
      if (Variable.UTC_TIMESTAMP.camelName.equals(name)) return this.utcTimestamp;

      try {
        int index = parameterToIndex.get(name);
        return switch (sources.get(index)) {
          case DOC_VALUE -> getFromDocValue((String) digests.get(index));
          case SOURCE -> getFromSource((String) digests.get(index));
          case LITERAL -> digests.get(index);
          case SPECIAL_VARIABLE ->
              // Special variables (state, states) are not in this context
              // They should be handled by ScriptedMetricDataContext
              throw new IllegalStateException(
                  "SPECIAL_VARIABLE " + digests.get(index) + " not supported in this context");
        };
      } catch (Exception e) {
        throw new IllegalStateException("Failed to get value for parameter " + name);
      }
    }

    public Object getFromDocValue(String name) {
      ScriptDocValues<?> docValue = this.docProvider.get(name);
      if (docValue == null || docValue.isEmpty()) {
        return null; // No way to differentiate null and missing from doc value
      }

      Object value = docValue.get(0);
      if (value instanceof ChronoZonedDateTime) {
        // We store timestamp as string in the current implementation with Calcite.
        // And the string should have the format defined in ExprTimestampValue
        // TODO: should we change to store timestamp as Instant in the future.
        return new ExprTimestampValue(((ChronoZonedDateTime<?>) value).toInstant()).value();
      }
      return value;
    }

    public Object getFromSource(String name) {
      return this.sourceLookup.get(name);
    }
  }

  @Getter
  public enum Source {
    DOC_VALUE(0),
    SOURCE(1),
    LITERAL(2),
    SPECIAL_VARIABLE(3); // For scripted metric state/states variables

    private final int value;

    Source(int value) {
      this.value = value;
    }

    private static final Map<Integer, Source> VALUE_TO_SOURCE = new HashMap<>();

    static {
      for (Source source : Source.values()) {
        VALUE_TO_SOURCE.put(source.value, source);
      }
    }

    public static Source fromValue(int value) {
      Source source = VALUE_TO_SOURCE.get(value);
      if (source == null) {
        throw new IllegalArgumentException("No Source with value: " + value);
      }
      return source;
    }
  }

  /**
   * This function is copied from Calcite RexExecutorImpl It's used to compile RexNode expression to
   * java code string.
   */
  public static String translate(
      RexBuilder rexBuilder,
      List<RexNode> constExps,
      RexToLixTranslator.InputGetter getter,
      RelDataType rowType) {
    RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, rexBuilder);
    java.util.Iterator var5 = constExps.iterator();

    while (var5.hasNext()) {
      RexNode node = (RexNode) var5.next();
      programBuilder.addProject(node, "c" + programBuilder.getProjectList().size());
    }

    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    JavaTypeFactory javaTypeFactory =
        typeFactory instanceof JavaTypeFactory
            ? (JavaTypeFactory) typeFactory
            : new JavaTypeFactoryImpl(typeFactory.getTypeSystem());
    BlockBuilder blockBuilder = new BlockBuilder();
    ParameterExpression root0_ = Expressions.parameter(Object.class, "root0");
    ParameterExpression root_ = DataContext.ROOT;
    blockBuilder.add(
        Expressions.declare(16, root_, Expressions.convert_(root0_, DataContext.class)));
    SqlConformance conformance = SqlConformanceEnum.DEFAULT;
    RexProgram program = programBuilder.getProgram();
    List<org.apache.calcite.linq4j.tree.Expression> expressions =
        RexToLixTranslator.translateProjects(
            program,
            (JavaTypeFactory) javaTypeFactory,
            conformance,
            blockBuilder,
            (BlockBuilder) null,
            (PhysType) null,
            root_,
            getter,
            (Function1) null);
    blockBuilder.add(
        Expressions.return_(
            (LabelTarget) null, Expressions.newArrayInit(Object[].class, expressions)));
    MethodDeclaration methodDecl =
        Expressions.methodDecl(
            1,
            Object[].class,
            BuiltInMethod.FUNCTION1_APPLY.method.getName(),
            ImmutableList.of(root0_),
            blockBuilder.toBlock());
    String code = Expressions.toString(methodDecl);
    if ((Boolean) CalciteSystemProperty.DEBUG.value()) {
      Util.debugCode(System.out, code);
    }

    return code;
  }
}
