/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.LabelTarget;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

/**
 * Utility for compiling Calcite RexNode expressions to executable Java code.
 *
 * <p>This class provides functionality to translate Calcite's row expressions (RexNode) into
 * executable Java code strings that can be compiled and executed. The translation process is based
 * on Calcite's RexExecutorImpl implementation.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * RexBuilder rexBuilder = // ... obtain RexBuilder
 * RexNode expression = // ... create expression
 * RelDataType rowType = // ... create row type
 *
 * // Create input getter for field access
 * RexToLixTranslator.InputGetter getter = (blockBuilder, index, storageType) -> {
 *   // Return expression for accessing field at index
 * };
 *
 * // Compile to Java code
 * String code = RexNodeCompiler.compile(rexBuilder, List.of(expression), getter, rowType);
 * }</pre>
 *
 * @see RexNode
 * @see RexBuilder
 * @see RexToLixTranslator
 */
public class RexNodeCompiler {

  /**
   * Compiles RexNode expressions to Java code string.
   *
   * <p>This method is based on Calcite's RexExecutorImpl and translates RexNode expressions into
   * executable Java code. The generated code can be compiled into a RexExecutable for efficient
   * evaluation.
   *
   * @param rexBuilder the RexBuilder for creating Rex expressions
   * @param constExps the list of RexNode expressions to compile
   * @param getter the InputGetter for accessing input fields
   * @param rowType the row type defining the input schema
   * @return Java code string that can be compiled and executed
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static String compile(
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
