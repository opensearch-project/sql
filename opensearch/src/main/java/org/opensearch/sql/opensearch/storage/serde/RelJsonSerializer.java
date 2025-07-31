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
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * A serializer that (de-)serializes Calcite RexNode, RelDataType and OpenSearch field mapping.
 *
 * <p>This serializer:
 * <li>Uses Calcite's RelJson class to convert RexNode and RelDataType to/from JSON string
 * <li>Manages required OpenSearch field mapping information Note: OpenSearch ExprType subclasses
 *     implement {@link java.io.Serializable} and are handled through standard Java serialization.
 */
@Getter
public class RelJsonSerializer {

  private final RelOptCluster cluster;

  public static final String EXPR = "expr";
  public static final String FIELD_TYPES = "fieldTypes";
  public static final String ROW_TYPE = "rowType";
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<>() {};
  private static final SqlOperatorTable pplSqlOperatorTable =
      SqlOperatorTables.chain(
          PPLBuiltinOperators.instance(),
          SqlStdOperatorTable.instance(),
          // Add a list of necessary SqlLibrary if needed
          SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
              SqlLibrary.MYSQL, SqlLibrary.BIG_QUERY, SqlLibrary.SPARK, SqlLibrary.POSTGRESQL));

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
   * <li>Convert RexNode and RelDataType objects to JSON strings.
   * <li>Combines these JSON strings with OpenSearch field mappings into a map
   * <li>Encodes the resulting map into a final object string
   *
   * @param rexNode pushed down RexNode
   * @param relDataType row type of RexNode input
   * @param fieldTypes input field and ExprType mapping
   * @return serialized string of map structure for inputs
   */
  public String serialize(
      RexNode rexNode, RelDataType relDataType, Map<String, ExprType> fieldTypes) {
    try {
      // Serialize RexNode and RelDataType by JSON
      JsonBuilder jsonBuilder = new JsonBuilder();
      RelJson relJson = RelJson.create().withJsonBuilder(jsonBuilder);
      String rexNodeJson = jsonBuilder.toJsonString(relJson.toJson(rexNode));
      String rowTypeJson = jsonBuilder.toJsonString(relJson.toJson(relDataType));
      // Construct envelope of serializable objects
      Map<String, Object> envelope =
          Map.of(EXPR, rexNodeJson, FIELD_TYPES, fieldTypes, ROW_TYPE, rowTypeJson);

      // Write bytes of all serializable contents
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(envelope);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize RexNode: " + rexNode, e);
    }
  }

  /**
   * Deserialize serialized map structure string into a map of RexNode, RelDataType and OpenSearch
   * field types.
   *
   * @param struct input serialized map structure string
   * @return map of RexNode, RelDataType and OpenSearch field types
   */
  public Map<String, Object> deserialize(String struct) {
    Map<String, Object> objectMap = null;
    try {
      // Recover Map object from bytes
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(struct));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      objectMap = (Map<String, Object>) objectInput.readObject();

      // PPL Expr types are all serializable
      Map<String, ExprType> fieldTypes = (Map<String, ExprType>) objectMap.get(FIELD_TYPES);
      // Deserialize RelDataType and RexNode by JSON
      RelJson relJson = RelJson.create();
      Map<String, Object> rowTypeMap = mapper.readValue((String) objectMap.get(ROW_TYPE), TYPE_REF);
      RelDataType rowType = relJson.toType(cluster.getTypeFactory(), rowTypeMap);
      OpenSearchRelInputTranslator inputTranslator = new OpenSearchRelInputTranslator(rowType);
      relJson = relJson.withInputTranslator(inputTranslator).withOperatorTable(pplSqlOperatorTable);
      Map<String, Object> exprMap = mapper.readValue((String) objectMap.get(EXPR), TYPE_REF);
      RexNode rexNode = relJson.toRex(cluster, exprMap);

      return Map.of(EXPR, rexNode, FIELD_TYPES, fieldTypes, ROW_TYPE, rowType);
    } catch (Exception e) {
      if (objectMap == null) {
        throw new IllegalStateException(
            "Failed to deserialize RexNode due to object map is null", e);
      }
      throw new IllegalStateException(
          "Failed to deserialize RexNode and its required structure: " + objectMap.get(EXPR), e);
    }
  }
}
