/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.node.NodeClient;
import org.opensearch.geospatial.action.IpEnrichmentActionClient;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;


/**
 * OpenSearch version of eval operator, which nodeClient instance from OpenSearch,
 * in order to perform OpenSearch related operation during the eval process.
 */
public class OpenSearchEvalOperator extends EvalOperator {

    @Getter
    private final NodeClient nodeClient;

    public OpenSearchEvalOperator(PhysicalPlan input, List<Pair<ReferenceExpression, Expression>> expressionList, NodeClient nodeClient) {
        super(input, expressionList);
        this.nodeClient = nodeClient;
    }

    @Override
    public ExprValue next() {
        ExprValue inputValue = this.getInput().next();
        Map<String, ExprValue> evalMap = eval(inputValue.bindingTuples());

        if (STRUCT == inputValue.type()) {
            ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
            Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
            for (Map.Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
                if (evalMap.containsKey(valueEntry.getKey())) {
                    resultBuilder.put(valueEntry.getKey(), evalMap.get(valueEntry.getKey()));
                    evalMap.remove(valueEntry.getKey());
                } else {
                    resultBuilder.put(valueEntry);
                }
            }
            resultBuilder.putAll(evalMap);
            return ExprTupleValue.fromExprValueMap(resultBuilder.build());
        } else {
            return inputValue;
        }
    }

    /**
     * Evaluate the expression in the {@link EvalOperator} with {@link Environment}.
     *
     * @param env {@link Environment}
     * @return The mapping of reference and {@link ExprValue} for each expression.
     */
    private Map<String, ExprValue> eval(Environment<Expression, ExprValue> env) {
        Map<String, ExprValue> evalResultMap = new LinkedHashMap<>();
        for (Pair<ReferenceExpression, Expression> pair : this.getExpressionList()) {
            ReferenceExpression var = pair.getKey();
            Expression valueExpr = pair.getValue();
            ExprValue value;
            if (valueExpr instanceof FunctionExpression &&
                "geoip".equals(((FunctionExpression) valueExpr).getFunctionName().getFunctionName())) {

                IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(nodeClient);

                String dataSource = ((FunctionExpression) valueExpr).getArguments().get(0).toString();
                dataSource = dataSource.substring(1, dataSource.length() - 1);
                String ipAddress = ((FunctionExpression) valueExpr).getArguments().get(1).toString();
                ipAddress = ipAddress.substring(1, ipAddress.length() - 1);
                try {
                    Map<String, Object> geoLocationData = ipClient.getGeoLocationData(ipAddress, dataSource);

                    geoLocationData.forEach((k,v) -> System.out.println(k + " " + v));
                    // Transform above into <String, ExprValue> , then do ExprTupleValue.fromExprValueMap

                    Map<String, ExprValue> collect = geoLocationData.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    v -> new ExprStringValue(v.toString())
                            ));

                    value = ExprTupleValue.fromExprValueMap(collect);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
//                if (ipAddress.equals("123")) {
//                    value = new ExprStringValue("custom logic");
//                } else {
//                    Map<String, ExprValue> stringImmutableMap = ImmutableMap.of(
//                            "key1", new ExprStringValue("value1"),
//                            "key2", new ExprStringValue("value2"));
//                    value = ExprTupleValue.fromExprValueMap(stringImmutableMap);
//                }
//                System.out.println("Custom logic");
            } else {
                value = pair.getValue().valueOf(env);
            }
            env = extendEnv(env, var, value);
            evalResultMap.put(var.toString(), value);
        }
        return evalResultMap;
    }



}
