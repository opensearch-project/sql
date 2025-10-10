/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;
//
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import org.apache.calcite.adapter.enumerable.NotNullImplementor;
// import org.apache.calcite.adapter.enumerable.NullPolicy;
// import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
// import org.apache.calcite.linq4j.tree.Expression;
// import org.apache.calcite.linq4j.tree.Expressions;
// import org.apache.calcite.linq4j.tree.Types;
// import org.apache.calcite.rel.type.RelDataType;
// import org.apache.calcite.rel.type.RelDataTypeFactory;
// import org.apache.calcite.rex.RexCall;
// import org.apache.calcite.sql.SqlOperatorBinding;
// import org.apache.calcite.sql.type.SqlReturnTypeInference;
// import org.apache.calcite.sql.type.SqlTypeName;
// import org.opensearch.sql.expression.function.ImplementorUDF;
// import org.opensearch.sql.expression.function.UDFOperandMetadata;
//
/// **
// * MVExpand function that expands multivalue (array) fields into multiple rows.
// */
// public class MVExpandFunctionImpl extends ImplementorUDF {
//
//    public MVExpandFunctionImpl() {
//        super(new MVExpandImplementor(), NullPolicy.ALL);
//    }
//
//    @Override
//    public SqlReturnTypeInference getReturnTypeInference() {
//        // For mvexpand, the output type should be the type of the array element (or ANY)
//        return sqlOperatorBinding -> {
//            RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
//
//            if (sqlOperatorBinding.getOperandCount() == 0) {
//                return typeFactory.createSqlType(SqlTypeName.NULL);
//            }
//
//            // Assume single argument: the array to expand
//            RelDataType operandType = sqlOperatorBinding.getOperandType(0);
//            RelDataType elementType =
//                    operandType.getComponentType() != null
//                            ? operandType.getComponentType()
//                            : typeFactory.createSqlType(SqlTypeName.ANY);
//
//            // Output is a scalar (not array)
//            return typeFactory.createTypeWithNullability(elementType, true);
//        };
//    }
//
//    @Override
//    public UDFOperandMetadata getOperandMetadata() {
//        return null;
//    }
//
//    public static class MVExpandImplementor implements NotNullImplementor {
//        @Override
//        public Expression implement(
//                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands)
// {
//            // Delegate to static Java method for value expansion
//            return Expressions.call(
//                    Types.lookupMethod(MVExpandFunctionImpl.class, "mvexpand", Object.class),
//                    translatedOperands.get(0));
//        }
//    }
//
//    /**
//     * Implementation for mvexpand.
//     * If the argument is a List, return its elements as a List (to be mapped to separate rows).
//     * If the argument is null or not a List, return a singleton list with the original value.
//     */
//    public static List<Object> mvexpand(Object arg) {
//        if (arg == null) {
//            return Collections.singletonList(null);
//        }
//        if (arg instanceof List<?>) {
//            List<?> arr = (List<?>) arg;
//            if (arr.isEmpty()) {
//                return Collections.singletonList(null);
//            }
//            return new ArrayList<>(arr);
//        } else {
//            // Non-array value: return as single-element list
//            return Collections.singletonList(arg);
//        }
//    }
// }
