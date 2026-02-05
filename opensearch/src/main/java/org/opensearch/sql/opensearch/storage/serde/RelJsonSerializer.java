/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine.OperatorTable;

/**
 * A serializer that (de-)serializes Calcite RexNode, RelDataType and OpenSearch field mapping.
 *
 * <p>This serializer:
 * <li>Uses Calcite's RelJson class to convert RexNode and RelDataType to/from JSON string
 * <li>Manages required OpenSearch field mapping information Note: OpenSearch ExprType subclasses
 *     implement {@link Serializable} and are handled through standard Java serialization.
 */
@Getter
public class RelJsonSerializer {

  private final RelOptCluster cluster;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<>() {};

  static {
    mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  public RelJsonSerializer(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Serializes Calcite expressions and field types into a map object string.
   *
   * <p>This method:
   * <li>Standardize the original RexNode
   * <li>Convert RexNode objects to JSON strings.
   * <li>Encodes the resulting map into a final object string
   *
   * @param rexNode pushed down RexNode
   * @return serialized string of RexNode expression.
   */
  public String serialize(RexNode rexNode, ScriptParameterHelper parameterHelper) {
    RexNode standardizedRexExpr =
        RexStandardizer.standardizeRexNodeExpression(rexNode, parameterHelper);
    try {
      // Serialize RexNode and RelDataType by JSON
      JsonBuilder jsonBuilder = new JsonBuilder();
      RelJson relJson = ExtendedRelJson.create(jsonBuilder);
      String rexNodeJson = jsonBuilder.toJsonString(relJson.toJson(standardizedRexExpr));

      if (CalcitePlanContext.skipEncoding.get()) return rexNodeJson;
      // Write bytes of all serializable contents
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(rexNodeJson);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize RexNode: " + standardizedRexExpr, e);
    }
  }

  /**
   * Deserialize serialized map structure string into a map of RexNode, RelDataType and OpenSearch
   * field types.
   *
   * @param struct input serialized map structure string
   * @return map of RexNode, RelDataType and OpenSearch field types
   */
  public RexNode deserialize(String struct) {
    String exprStr = null;
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(struct));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      exprStr = (String) objectInput.readObject();

      // Deserialize RelDataType and RexNode by JSON
      RelJson relJson = ExtendedRelJson.create((JsonBuilder) null);
      relJson =
          relJson
              .withInputTranslator(ExtendedRelJson::translateInput)
              .withOperatorTable(OperatorTable.getChainedOperatorTable());
      Map<String, Object> exprMap = mapper.readValue(exprStr, TYPE_REF);
      return relJson.toRex(cluster, exprMap);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize RexNode " + exprStr, e);
    }
  }
}
