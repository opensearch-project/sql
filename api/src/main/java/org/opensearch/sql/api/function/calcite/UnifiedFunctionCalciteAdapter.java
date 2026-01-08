/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.opensearch.sql.api.function.UnifiedFunction;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Adapter that implements {@link UnifiedFunction} interface with Calcite-specific execution logic.
 *
 * <p>This adapter wraps Calcite's {@link RexExecutable} to provide engine-agnostic function
 * evaluation. It handles serialization by storing the generated Java code string and recreating the
 * executable during deserialization.
 *
 * <h2>Serialization Strategy</h2>
 *
 * <p>Calcite's {@link RexExecutable} is not directly serializable due to Guava cache issues. This
 * adapter works around this limitation by:
 *
 * <ol>
 *   <li>Storing the generated Java code string during serialization
 *   <li>Recreating the {@link RexExecutable} from the code during deserialization
 *   <li>Marking the {@link RexExecutable} field as transient to exclude it from default
 *       serialization
 * </ol>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // Create adapter for UPPER function
 * RexBuilder rexBuilder = // ... obtain RexBuilder
 * RexNode inputRef = rexBuilder.makeInputRef(varcharType, 0);
 * UnifiedFunction upperFunc = UnifiedFunctionCalciteAdapter.create(
 *     "UPPER",
 *     rexBuilder,
 *     List.of(inputRef)
 * );
 *
 * // Evaluate function
 * Object result = upperFunc.eval(List.of("hello"));  // "HELLO"
 *
 * // Serialize and deserialize
 * byte[] serialized = // ... serialize upperFunc
 * UnifiedFunction deserialized = // ... deserialize
 * Object result2 = deserialized.eval(List.of("world"));  // "WORLD"
 * }</pre>
 *
 * @see UnifiedFunction
 * @see RexExecutable
 * @see RexNodeCompiler
 */
public class UnifiedFunctionCalciteAdapter implements UnifiedFunction {

  private static final long serialVersionUID = 1L;

  private final String functionName;
  private transient RexExecutable rexExecutor;
  private String serializedCode;
  private final String returnTypeName;
  private final List<String> inputTypeNames;
  private final boolean isNullable;

  /**
   * Private constructor for creating adapter instances.
   *
   * <p>Use the {@link #create(String, RexBuilder, List)} factory method to create instances.
   *
   * @param functionName the name of the function
   * @param rexExecutor the compiled Calcite executable
   * @param serializedCode the generated Java code string
   * @param returnTypeName the SQL type name of the return type
   * @param inputTypeNames the SQL type names of the input types
   * @param isNullable whether the function can return null
   */
  private UnifiedFunctionCalciteAdapter(
      String functionName,
      RexExecutable rexExecutor,
      String serializedCode,
      String returnTypeName,
      List<String> inputTypeNames,
      boolean isNullable) {
    this.functionName = Objects.requireNonNull(functionName, "functionName must not be null");
    this.rexExecutor = Objects.requireNonNull(rexExecutor, "rexExecutor must not be null");
    this.serializedCode = Objects.requireNonNull(serializedCode, "serializedCode must not be null");
    this.returnTypeName = Objects.requireNonNull(returnTypeName, "returnTypeName must not be null");
    this.inputTypeNames = Objects.requireNonNull(inputTypeNames, "inputTypeNames must not be null");
    this.isNullable = isNullable;
  }

  /**
   * Factory method to create a {@link UnifiedFunctionCalciteAdapter} from a function name and
   * Calcite RexNode children.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Resolves the function from {@link PPLFuncImpTable} using the function name and argument
   *       types
   *   <li>Creates a row type from the RexNode operands for compilation
   *   <li>Uses {@link CalciteScriptEngine} to translate the RexNode to Java code
   *   <li>Compiles the code into a {@link RexExecutable}
   *   <li>Extracts input and return types using {@link CalciteTypeConverter}
   *   <li>Returns a configured adapter instance
   * </ol>
   *
   * <p>Example:
   *
   * <pre>{@code
   * RexBuilder rexBuilder = // ... obtain RexBuilder
   * RexNode input = rexBuilder.makeInputRef(varcharType, 0);
   * UnifiedFunction concatFunc = UnifiedFunctionCalciteAdapter.create(
   *     "CONCAT",
   *     rexBuilder,
   *     List.of(input, rexBuilder.makeLiteral("suffix"))
   * );
   * }</pre>
   *
   * @param functionName the name of the PPL function to adapt (e.g., "UPPER", "CONCAT", "ABS")
   * @param rexBuilder RexBuilder for creating Rex expressions
   * @param rexNodes Calcite RexNode children representing function arguments
   * @return a configured UnifiedFunctionCalciteAdapter instance
   * @throws IllegalArgumentException if the function cannot be resolved or if arguments are invalid
   * @throws NullPointerException if any parameter is null
   */
  public static UnifiedFunctionCalciteAdapter create(
      String functionName, RexBuilder rexBuilder, List<RexNode> rexNodes) {
    Objects.requireNonNull(functionName, "functionName must not be null");
    Objects.requireNonNull(rexBuilder, "rexBuilder must not be null");
    Objects.requireNonNull(rexNodes, "rexNodes must not be null");

    var typeFactory = rexBuilder.getTypeFactory();

    // Resolve the PPL function with actual argument types
    RexNode rexNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder, functionName, rexNodes.toArray(new RexNode[0]));

    // Pre-compile RexExecutable to avoid Guava deserialization issues
    RelDataType rowType;
    if (!(rexNode instanceof RexCall)) {
      rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
    } else {
      RexCall rexCall = (RexCall) rexNode;
      List<RexNode> operands = rexCall.getOperands();

      if (operands.isEmpty()) {
        rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      } else {
        List<RexInputRef> inputRefs = new ArrayList<>();
        for (RexNode operand : operands) {
          if (operand instanceof RexInputRef) {
            inputRefs.add((RexInputRef) operand);
          }
        }

        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (RexInputRef ref : inputRefs) {
          types.add(ref.getType());
          names.add("_" + ref.getIndex());
        }
        rowType = typeFactory.createStructType(types, names);
      }
    }

    RexToLixTranslator.InputGetter getter =
        (blockBuilder, index, storageType) ->
            Expressions.field(
                Expressions.convert_(
                    Expressions.call(
                        DataContext.ROOT,
                        BuiltInMethod.DATA_CONTEXT_GET.method,
                        Expressions.constant("_" + index)),
                    Object.class),
                "value");
    String code =
        translateRexToCode(rexBuilder, Collections.singletonList(rexNode), getter, rowType);
    RexExecutable rexExecutor = new RexExecutable(code, "Unified function generated code");

    // Extract input types from rexNodes and convert to SQL type name strings
    List<String> inputTypeNames = new ArrayList<>();
    for (RexNode node : rexNodes) {
      inputTypeNames.add(CalciteTypeConverter.relDataTypeToSqlTypeName(node.getType()));
    }
    String returnTypeName = CalciteTypeConverter.relDataTypeToSqlTypeName(rexNode.getType());
    boolean isNullable = rexNode.getType().isNullable();

    return new UnifiedFunctionCalciteAdapter(
        functionName, rexExecutor, code, returnTypeName, inputTypeNames, isNullable);
  }

  @Override
  public String getFunctionName() {
    return functionName;
  }

  @Override
  public List<String> getInputTypes() {
    return new ArrayList<>(inputTypeNames);
  }

  @Override
  public String getReturnType() {
    return returnTypeName;
  }

  @Override
  public boolean isNullable() {
    return isNullable;
  }

  @Override
  public Object eval(List<Object> inputs) {
    Objects.requireNonNull(inputs, "inputs must not be null");

    // Validate input count
    if (inputs.size() != inputTypeNames.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Function '%s' expects %d arguments but got %d",
              functionName, inputTypeNames.size(), inputs.size()));
    }

    // Create DataContext with input values
    DataContext dataContext = createDataContext(inputs);
    rexExecutor.setDataContext(dataContext);

    // Execute the function
    try {
      Object[] results = rexExecutor.execute();
      // The result is an array with one element (the function result)
      return (results == null || results.length == 0) ? null : results[0];
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to evaluate function '%s': %s", functionName, e.getMessage()), e);
    }
  }

  /**
   * Creates a DataContext for function evaluation.
   *
   * <p>The DataContext provides access to input values through field names following the pattern
   * "_0", "_1", "_2", etc., matching the RexInputRef indices used during compilation.
   *
   * @param inputs the input values to make available in the context
   * @return a DataContext containing the input values
   */
  private DataContext createDataContext(List<Object> inputs) {
    Map<String, Object> fieldMap = new HashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      Object value = inputs.get(i);
      fieldMap.put("_" + i, (value == null) ? null : value);
    }
    return DataContexts.of(fieldMap);
  }

  /**
   * Custom serialization to handle the non-serializable {@link RexExecutable}.
   *
   * <p>This method serializes the generated code string instead of the RexExecutable itself.
   *
   * @param out the output stream
   * @throws IOException if an I/O error occurs
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  /**
   * Custom deserialization to recreate the {@link RexExecutable} from the serialized code.
   *
   * <p>This method recreates the RexExecutable by compiling the stored code string.
   *
   * @param in the input stream
   * @throws IOException if an I/O error occurs
   * @throws ClassNotFoundException if a required class cannot be found
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // Recreate RexExecutable from serialized code
    this.rexExecutor = new RexExecutable(serializedCode, "deserialized Rex code");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnifiedFunctionCalciteAdapter that = (UnifiedFunctionCalciteAdapter) o;
    return isNullable == that.isNullable
        && Objects.equals(functionName, that.functionName)
        && Objects.equals(returnTypeName, that.returnTypeName)
        && Objects.equals(inputTypeNames, that.inputTypeNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName, returnTypeName, inputTypeNames, isNullable);
  }

  @Override
  public String toString() {
    return String.format(
        "UnifiedFunctionCalciteAdapter{functionName='%s', inputTypes=%s, returnType='%s',"
            + " nullable=%s}",
        functionName, inputTypeNames, returnTypeName, isNullable);
  }

  /**
   * Translates RexNode expressions to Java code string.
   *
   * <p>This method is adapted from Calcite's RexExecutorImpl to compile RexNode expressions into
   * executable Java code.
   *
   * @param rexBuilder RexBuilder for creating expressions
   * @param constExps List of RexNode expressions to translate
   * @param getter InputGetter for accessing input values
   * @param rowType Row type for the input
   * @return Java code string that can be compiled into a RexExecutable
   */
  private static String translateRexToCode(
      RexBuilder rexBuilder,
      List<RexNode> constExps,
      RexToLixTranslator.InputGetter getter,
      RelDataType rowType) {
    RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, rexBuilder);

    for (RexNode node : constExps) {
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

    @SuppressWarnings("unchecked")
    List<org.apache.calcite.linq4j.tree.Expression> expressions =
        RexToLixTranslator.translateProjects(
            program, javaTypeFactory, conformance, blockBuilder, null, null, root_, getter, null);

    blockBuilder.add(
        Expressions.return_(null, Expressions.newArrayInit(Object[].class, expressions)));

    MethodDeclaration methodDecl =
        Expressions.methodDecl(
            1,
            Object[].class,
            BuiltInMethod.FUNCTION1_APPLY.method.getName(),
            ImmutableList.of(root0_),
            blockBuilder.toBlock());

    String code = Expressions.toString(methodDecl);
    if (Boolean.TRUE.equals(CalciteSystemProperty.DEBUG.value())) {
      Util.debugCode(System.out, code);
    }

    return code;
  }
}
