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

import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.time.chrono.ChronoZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.LabelTarget;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.script.FilterScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.NamedFieldExpression;
import org.opensearch.sql.opensearch.storage.script.filter.CalciteFilterScriptFactory;

/**
 * Custom expression script engine that supports using core engine expression code in DSL as a new
 * script language just like built-in Painless language.
 */
@RequiredArgsConstructor
public class CalciteScriptEngine implements ScriptEngine {

  /** Expression script language name. */
  public static final String CALCITE_LANG_NAME = "opensearch_calcite";

  /** All supported script contexts and function to create factory from expression. */
  private static final Map<ScriptContext<?>, Function<Function1<DataContext, Object[]>, Object>>
      CONTEXTS =
          new ImmutableMap.Builder<
                  ScriptContext<?>, Function<Function1<DataContext, Object[]>, Object>>()
              .put(FilterScript.CONTEXT, CalciteFilterScriptFactory::new)
              .build();

  @Override
  public String getType() {
    return CALCITE_LANG_NAME;
  }

  @Override
  public <T> T compile(
      String scriptName, String scriptCode, ScriptContext<T> context, Map<String, String> options) {
    Function1<DataContext, Object[]> function = compile(scriptCode, "generated Rex code");

    if (CONTEXTS.containsKey(context)) {
      return context.factoryClazz.cast(CONTEXTS.get(context).apply(function));
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

  /**
   * Implementation of {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter}
   * that reads the values of input fields by calling <code>
   * {@link org.apache.calcite.DataContext#get}("inputRecord")</code>.
   */
  public static class ScriptInputGetter implements InputGetter {
    private final RelDataTypeFactory typeFactory;
    private final RelDataType rowType;
    private final Map<String, ExprType> fieldTypes;

    public ScriptInputGetter(
        RelDataTypeFactory typeFactory, RelDataType rowType, Map<String, ExprType> fieldTypes) {
      this.typeFactory = typeFactory;
      this.rowType = rowType;
      this.fieldTypes = fieldTypes;
    }

    @Override
    public org.apache.calcite.linq4j.tree.Expression field(
        BlockBuilder list, int index, @Nullable Type storageType) {
      String fieldName = rowType.getFieldList().get(index).getName();
      ExprType exprType = fieldTypes.get(fieldName);
      if (exprType == ExprCoreType.STRUCT) {
        throw new UnsupportedScriptException(
            "Script query does not support fields of struct type: " + fieldName);
      }
      NamedFieldExpression expression = new NamedFieldExpression(fieldName, exprType);
      String referenceField = expression.getReferenceForTermQuery();
      if (StringUtils.isEmpty(referenceField)) {
        throw new UnsupportedScriptException(
            "Field name cannot be empty for expression: " + expression);
      }
      MethodCallExpression fieldValueExpr =
          Expressions.call(
              DataContext.ROOT,
              BuiltInMethod.DATA_CONTEXT_GET.method,
              Expressions.constant(expression.getReferenceForTermQuery()));
      if (storageType == null) {
        final RelDataType fieldType = rowType.getFieldList().get(index).getType();
        storageType = ((JavaTypeFactory) typeFactory).getJavaClass(fieldType);
      }
      return EnumUtils.convert(tryConvertDocValue(fieldValueExpr, exprType), storageType);
    }

    /**
     * DocValue only support long and double for integer and float, cast to the related type first
     */
    private Expression tryConvertDocValue(Expression docValueExpr, ExprType exprType) {
      return switch (exprType) {
        case INTEGER -> EnumUtils.convert(docValueExpr, Long.class);
        case FLOAT -> EnumUtils.convert(docValueExpr, Double.class);
        default -> docValueExpr;
      };
    }
  }

  public static class ScriptDataContext implements DataContext {

    private final Supplier<Map<String, ScriptDocValues<?>>> docProvider;
    private final Map<String, Object> params;

    public ScriptDataContext(
        Supplier<Map<String, ScriptDocValues<?>>> docProvider, Map<String, Object> params) {
      this.docProvider = docProvider;
      this.params = params;
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
      if (Variable.UTC_TIMESTAMP.camelName.equals(name))
        return params.get(Variable.UTC_TIMESTAMP.camelName);

      ScriptDocValues<?> docValue = docProvider.get().get(name);
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

  /**
   * This function is copied from Calcite RexExecutable It's used to compile java code string to
   * java function.
   */
  public static Function1<DataContext, Object[]> compile(String code, Object reason) {
    try {
      ClassBodyEvaluator cbe = new ClassBodyEvaluator();
      cbe.setClassName("Reducer");
      cbe.setExtendedClass(Utilities.class);
      cbe.setImplementedInterfaces(new Class[] {Function1.class, Serializable.class});
      cbe.setParentClassLoader(RexExecutable.class.getClassLoader());
      cbe.cook(new Scanner((String) null, new StringReader(code)));
      Class c = cbe.getClazz();
      Constructor<Function1<DataContext, Object[]>> constructor = c.getConstructor();
      return (Function1) constructor.newInstance();
    } catch (IOException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | CompileException var5) {
      Exception e = var5;
      throw new RuntimeException("While compiling " + reason, e);
    }
  }
}
