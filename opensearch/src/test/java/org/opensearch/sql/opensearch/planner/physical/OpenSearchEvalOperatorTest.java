/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.geospatial.action.IpEnrichmentAction;
import org.opensearch.geospatial.action.IpEnrichmentRequest;
import org.opensearch.geospatial.action.IpEnrichmentResponse;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.ip.OpenSearchFunctionExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprTextValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** To assert the original behaviour of eval operator. */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@RunWith(MockitoJUnitRunner.Silent.class)
public class OpenSearchEvalOperatorTest {

  @Mock private PhysicalPlan input;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ActionFuture<ActionResponse> actionFuture;

  private final ExprTupleValue DATE_ROW =
      new ExprTupleValue(
          new LinkedHashMap<>(
              Map.of(
                  "firstname",
                  new OpenSearchExprTextValue("Amber"),
                  "age",
                  new ExprLongValue(32),
                  "email",
                  new OpenSearchExprTextValue("amberduke@pyrami.com"),
                  "ipInStr",
                  new OpenSearchExprTextValue("192.168.1.1"))));

  /**
   * The test-case aim to assert OpenSearchEvalOperator behaviour when evaluating generic
   * expression, which is not OpenSearch Engine specific. (Ex: ABS(age) )
   */
  @Test
  public void testEvalOperatorOnGenericOperations() {

    // The input dataset
    when(input.next()).thenReturn(DATE_ROW);

    // Expression to be evaluated
    List<Pair<ReferenceExpression, Expression>> ipAddress =
        List.of(
            ImmutablePair.of(
                new ReferenceExpression("ageInAbs", OpenSearchTextType.of()),
                DSL.abs(DSL.abs(new ReferenceExpression("age", ExprCoreType.LONG)))));

    OpenSearchEvalOperator evalOperator = new OpenSearchEvalOperator(input, ipAddress, nodeClient);

    // Make sure generic Expression function as expected when wrapped with OpenSearchEvalOperator.
    assertSame(32, evalOperator.next().keyValue("ageInAbs").integerValue());
  }

  /**
   * The test-case aim to assert OpenSearchEvalOperator behaviour when evaluating
   * geoipFunctionExpression, which is only available when executing on OpeSearch storage engine.
   * (No option)
   */
  @SneakyThrows
  @Test
  public void testEvalOperatorOnGeoIpExpression() {

    // The input dataset
    when(input.next()).thenReturn(DATE_ROW);
    when(nodeClient.execute(
            eq(IpEnrichmentAction.INSTANCE),
            argThat(
                request ->
                    request instanceof IpEnrichmentRequest
                        && "192.168.1.1".equals(((IpEnrichmentRequest) request).getIpString()))))
        .thenReturn(actionFuture);
    when(actionFuture.get()).thenReturn(new IpEnrichmentResponse(Map.of("country_name", "Canada")));

    // Expression to be evaluated
    List<Pair<ReferenceExpression, Expression>> ipAddress =
        List.of(
            ImmutablePair.of(
                new ReferenceExpression("ipEnrichmentResult", OpenSearchTextType.of()),
                new OpenSearchFunctionExpression(
                    BuiltinFunctionName.GEOIP.getName(),
                    List.of(
                        DSL.literal("my-datasource"),
                        new ReferenceExpression("ipInStr", OpenSearchTextType.of())),
                    BOOLEAN)));

    OpenSearchEvalOperator evalOperator = new OpenSearchEvalOperator(input, ipAddress, nodeClient);

    // Make sure generic Expression function as expected when wrapped with OpenSearchEvalOperator.
    Map<String, ExprValue> ipEnrichmentResult =
        evalOperator.next().keyValue("ipEnrichmentResult").tupleValue();
    assertSame("Canada", ipEnrichmentResult.get("country_name").stringValue());
  }
}
