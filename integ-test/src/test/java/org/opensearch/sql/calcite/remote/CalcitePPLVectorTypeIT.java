/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.util.Capability.VECTOR_SEARCH;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.getResponseBody;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Reading {@code knn_vector} fields as the {@code vector} type on the Calcite engine: projection,
 * the reported column type, joins with a second index, null handling, and the <i>vector not
 * comparable</i> error.
 *
 * <p>The default integ-test cluster does not install the k-NN plugin, so every test is skipped
 * through {@link Assume#assumeTrue} when it is absent, as in {@code VectorSearchExecutionIT}. To
 * run it, start the cluster with the k-NN plugin: {@code ./gradlew :integ-test:integTest
 * -Pknn.integ=true -Dtests.class="*CalcitePPLVectorTypeIT"}.
 */
@RequiresCapability(VECTOR_SEARCH)
public class CalcitePPLVectorTypeIT extends PPLIntegTestCase {

  private static final String VECTOR_INDEX = "ppl_vector_type_vectors";
  private static final String DOCS_INDEX = "ppl_vector_type_docs";
  private static final String EXCLUDED_INDEX = "ppl_vector_type_excluded";
  private static final String PRECISION_INDEX = "ppl_vector_type_precision";
  private static final String STORED_INDEX = "ppl_vector_type_stored";
  private static final String MIXED_FLOAT_INDEX = "ppl_vector_type_mixed_float";
  private static final String MIXED_BYTE_INDEX = "ppl_vector_type_mixed_byte";
  private static final String MERGE_INDICES = "ppl_vector_type_merge_*";
  private static final String TOP_HITS_INDEX = "ppl_vector_type_top_hits";

  private static final String LUCENE_HNSW =
      "\"method\": {\"name\": \"hnsw\", \"engine\": \"lucene\", \"space_type\": \"l2\"}";

  /**
   * Derived source off; a float vector f and a byte vector b, as JSON arrays in doc 1 and as base64
   * in doc 2: f = 1.0f, -2.5f, 2.0f little-endian float32, b = the bytes 1, 2, -1.
   */
  private static final String TOP_HITS_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true, \"knn.derived_source.enabled\": false}},"
          + " \"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
          + " \"g\": {\"type\": \"keyword\"}, \"ts\": {\"type\": \"date\"},"
          + " \"f\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}, \"b\": {\"type\": \"knn_vector\", \"dimension\": 3, \"data_type\": \"byte\", "
          + LUCENE_HNSW
          + "}}}}";

  private static final String TOP_HITS_DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"g\": \"a\", \"ts\": \"2020-01-01\", \"f\": [1, 2.5, 3],"
          + " \"b\": [1, -2, 3]}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"g\": \"b\", \"ts\": \"2020-01-02\", \"f\": \"AACAPwAAIMAAAABA\","
          + " \"b\": \"AQL/\"}\n";

  /** Derived source off; a float vector at the top level and in an object, in a routed document. */
  private static final String ROUTED_INDEX = "ppl_vector_type_routed";

  private static final String ROUTED_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true, \"knn.derived_source.enabled\": false}},"
          + " \"mappings\": {\"properties\": {\"g\": {\"type\": \"keyword\"},"
          + " \"ts\": {\"type\": \"date\"}, \"f\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}, \"obj\": {\"properties\": {\"vec\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}}}}}}";

  private static final String ROUTED_DOCS =
      "{\"index\": {\"_id\": \"1\", \"routing\": \"r1\"}}\n"
          + "{\"g\": \"a\", \"ts\": \"2020-01-01\", \"f\": [1, 2.5, 3], \"obj\": {\"vec\": [4, 5,"
          + " 6]}}\n";

  /**
   * Derived source on (the default): k-NN keeps the vectors out of the stored _source and injects
   * them when a document is fetched, also inside an object.
   */
  private static final String DERIVED_INDEX = "ppl_vector_type_derived";

  private static final String DERIVED_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true}},"
          + " \"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
          + " \"g\": {\"type\": \"keyword\"}, \"ts\": {\"type\": \"date\"},"
          + " \"f\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}, \"b\": {\"type\": \"knn_vector\", \"dimension\": 3, \"data_type\": \"byte\", "
          + LUCENE_HNSW
          + "}, \"obj\": {\"properties\": {\"vec\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}, \"name\": {\"type\": \"keyword\"}}}}}}";

  private static final String DERIVED_DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"g\": \"a\", \"ts\": \"2020-01-01\", \"f\": [1, 2.5, 3],"
          + " \"b\": [1, -2, 3], \"obj\": {\"vec\": [4, 5, 6], \"name\": \"x\"}}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"g\": \"b\", \"ts\": \"2020-01-02\", \"f\": [0.5, 1, 2],"
          + " \"b\": [1, 2, -1], \"obj\": {\"vec\": [7, 8, 9], \"name\": \"y\"}}\n";

  /**
   * Derived source off, with an alias to a vector, vectors inside an object and a nested field, and
   * base64 vectors, which _source then keeps as strings.
   */
  private static final String STORED_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true, \"knn.derived_source.enabled\": false}},"
          + " \"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
          + " \"embedding\": {\"type\": \"knn_vector\", \"dimension\": 3, "
          + LUCENE_HNSW
          + "}, \"emb_alias\": {\"type\": \"alias\", \"path\": \"embedding\"},"
          + " \"obj\": {\"properties\": {\"vec\": {\"type\": \"knn_vector\", \"dimension\": 2, "
          + LUCENE_HNSW
          + "}, \"name\": {\"type\": \"keyword\"}}},"
          + " \"nest\": {\"type\": \"nested\", \"properties\": {\"vec\": {\"type\":"
          + " \"knn_vector\", \"dimension\": 2, "
          + LUCENE_HNSW
          + "}, \"n\": {\"type\": \"integer\"}}},"
          + " \"b64\": {\"type\": \"knn_vector\", \"dimension\": 2, "
          + LUCENE_HNSW
          + "}, \"bytes64\": {\"type\": \"knn_vector\", \"dimension\": 2, \"data_type\": \"byte\", "
          + LUCENE_HNSW
          + "}}}}";

  /** b64 is 1.0f, -2.5f as little-endian float32; bytes64 is the bytes 1, -1. */
  private static final String STORED_DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"embedding\": [1, 2, 3], \"obj\": {\"vec\": [1, 2], \"name\": \"x\"},"
          + " \"b64\": \"AACAPwAAIMA=\", \"bytes64\": \"Af8=\"}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"nest\": [{\"vec\": [1, 2], \"n\": 1}, {\"vec\": [3, 4], \"n\":"
          + " 2}]}\n"
          + "{\"index\": {\"_id\": \"3\"}}\n"
          + "{\"doc_id\": 3, \"nest\": [{\"vec\": \"AACAPwAAIMA=\", \"n\": 1}]}\n";

  /**
   * Derived source off, field {@code v} of dimension 4 with the given data type: two indices that
   * disagree on whether the elements are bytes.
   */
  private static String mixedMapping(String dataType) {
    return "{\"settings\": {\"index\": {\"knn\": true, \"knn.derived_source.enabled\": false}},"
        + " \"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
        + " \"v\": {\"type\": \"knn_vector\", \"dimension\": 4, \"data_type\": \""
        + dataType
        + "\", "
        + LUCENE_HNSW
        + "}, \"obj\": {\"properties\": {\"vec\": {\"type\": \"knn_vector\", \"dimension\": 4,"
        + " \"data_type\": \""
        + dataType
        + "\", "
        + LUCENE_HNSW
        + "}}}}}}";
  }

  /**
   * Derived source off, so _source keeps the vectors as written: integer elements and decimals with
   * more significant digits than a float holds.
   */
  private static final String PRECISION_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true, \"knn.derived_source.enabled\": false}},"
          + " \"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
          + " \"embedding\": {\"type\": \"knn_vector\", \"dimension\": 3,"
          + " \"method\": {\"name\": \"hnsw\", \"engine\": \"lucene\", \"space_type\": \"l2\"}}}}}";

  private static final String PRECISION_DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"embedding\": [1, 2, 3]}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"embedding\": [-0.006929283495992422, 0.123456789012, 3.5]}\n";

  private static final String VECTOR_FIELDS =
      "\"doc_id\": {\"type\": \"integer\"},"
          + "\"embedding\": {\"type\": \"knn_vector\", \"dimension\": 3,"
          + " \"method\": {\"name\": \"hnsw\", \"engine\": \"lucene\", \"space_type\": \"l2\"}},"
          + "\"code\": {\"type\": \"knn_vector\", \"dimension\": 2, \"data_type\": \"byte\","
          + " \"method\": {\"name\": \"hnsw\", \"engine\": \"lucene\", \"space_type\": \"l2\"}}";

  private static final String VECTOR_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true}},"
          + " \"mappings\": {\"properties\": {"
          + VECTOR_FIELDS
          + "}}}";

  private static final String EXCLUDED_MAPPING =
      "{\"settings\": {\"index\": {\"knn\": true}},"
          + " \"mappings\": {\"_source\": {\"excludes\": [\"embedding\"]}, \"properties\": {"
          + VECTOR_FIELDS
          + "}}}";

  private static final String VECTOR_DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"embedding\": [1.0, 2.0, 3.0], \"code\": [1, -2]}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"embedding\": [0.5, 0.25, -1.0], \"code\": [127, -128]}\n"
          + "{\"index\": {\"_id\": \"3\"}}\n"
          + "{\"doc_id\": 3}\n";

  private static final String DOCS =
      "{\"index\": {\"_id\": \"1\"}}\n"
          + "{\"doc_id\": 1, \"title\": \"first\"}\n"
          + "{\"index\": {\"_id\": \"2\"}}\n"
          + "{\"doc_id\": 2, \"title\": \"second\"}\n"
          + "{\"index\": {\"_id\": \"4\"}}\n"
          + "{\"doc_id\": 4, \"title\": \"fourth\"}\n";

  @Override
  public void init() throws Exception {
    super.init();
    Assume.assumeTrue("k-NN plugin not installed on test cluster", isKnnPluginInstalled());
    enableCalcite();
    createAndLoad(VECTOR_INDEX, VECTOR_MAPPING, VECTOR_DOCS);
    createAndLoad(
        DOCS_INDEX,
        "{\"mappings\": {\"properties\": {\"doc_id\": {\"type\": \"integer\"},"
            + " \"title\": {\"type\": \"keyword\"}}}}",
        DOCS);
    createAndLoad(
        EXCLUDED_INDEX,
        EXCLUDED_MAPPING,
        "{\"index\": {\"_id\": \"1\"}}\n"
            + "{\"doc_id\": 1, \"embedding\": [1.0, 2.0, 3.0], \"code\": [1, -2]}\n");
    createAndLoad(PRECISION_INDEX, PRECISION_MAPPING, PRECISION_DOCS);
    createAndLoad(STORED_INDEX, STORED_MAPPING, STORED_DOCS);
    // 1.0f, -2.5f, 2.0f, 3.0f as little-endian float32, and an array.
    createAndLoad(
        MIXED_FLOAT_INDEX,
        mixedMapping("float"),
        "{\"index\": {\"_id\": \"1\"}}\n"
            + "{\"doc_id\": 1, \"v\": \"AACAPwAAIMAAAABAAABAQA==\","
            + " \"obj\": {\"vec\": \"AACAPwAAIMAAAABAAABAQA==\"}}\n"
            + "{\"index\": {\"_id\": \"3\"}}\n"
            + "{\"doc_id\": 3, \"v\": [0.5, 1, 2, 3]}\n");
    // The bytes 1, 2, 3, -1.
    createAndLoad(
        MIXED_BYTE_INDEX,
        mixedMapping("byte"),
        "{\"index\": {\"_id\": \"2\"}}\n"
            + "{\"doc_id\": 2, \"v\": \"AQID/w==\", \"obj\": {\"vec\": \"AQID/w==\"}}\n");
    createAndLoad(TOP_HITS_INDEX, TOP_HITS_MAPPING, TOP_HITS_DOCS);
    createAndLoad(DERIVED_INDEX, DERIVED_MAPPING, DERIVED_DOCS);
    createAndLoad(ROUTED_INDEX, ROUTED_MAPPING, ROUTED_DOCS);
    // Field f is a knn_vector of dimension 768 or 384 in two indices and nested in a third.
    for (String[] index :
        List.of(
            new String[] {"knn768", "{\"type\": \"knn_vector\", \"dimension\": 768}"},
            new String[] {"knn384", "{\"type\": \"knn_vector\", \"dimension\": 384}"},
            new String[] {
              "nested", "{\"type\": \"nested\", \"properties\": {\"sub\": {\"type\": \"integer\"}}}"
            })) {
      createAndLoad(
          MERGE_INDICES.replace("*", index[0]),
          "{\"settings\": {\"index\": {\"knn\": true}}, \"mappings\": {\"properties\":"
              + " {\"doc_id\": {\"type\": \"integer\"}, \"f\": "
              + index[1]
              + "}}}",
          "{\"index\": {\"_id\": \"1\"}}\n{\"doc_id\": 1}\n");
    }
  }

  /**
   * On Calcite, vectors in objects and nested fields, base64 vectors and the alias column all have
   * float (or byte) elements.
   */
  @Test
  public void testCalciteConvertsNestedBase64AndAliasVectors() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where doc_id = 1 | fields obj, obj.vec, b64, bytes64, emb_alias",
                STORED_INDEX));
    verifySchema(
        result,
        schema("obj", "struct"),
        schema("obj.vec", "vector"),
        schema("b64", "vector"),
        schema("bytes64", "vector"),
        schema("emb_alias", "vector"));
    assertEquals(
        "[[{\"vec\":[1.0,2.0],\"name\":\"x\"},[1.0,2.0],[1.0,-2.5],[1,-1],[1.0,2.0,3.0]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | where doc_id = 1 | fields obj, obj.vec, b64, bytes64, emb_alias",
                    STORED_INDEX))));
    assertEquals(
        "[[[{\"vec\":[1.0,2.0],\"n\":1},{\"vec\":[3.0,4.0],\"n\":2}]]]",
        rawDatarows(
            executeQueryToString(
                String.format("source=%s | where doc_id = 2 | fields nest", STORED_INDEX))));
    assertEquals(
        "[[[{\"vec\":[1.0,-2.5],\"n\":1}]]]",
        rawDatarows(
            executeQueryToString(
                String.format("source=%s | where doc_id = 3 | fields nest", STORED_INDEX))));
  }

  /** SQL and the V2 engine read these as before: stored numbers, base64 as an empty array. */
  @Test
  public void testSqlAndV2ReadNestedBase64AndAliasVectorsAsBefore() throws IOException {
    String asBefore = "[[{\"vec\":[1,2],\"name\":\"x\"},[],[],[1,2,3]]]";
    JSONObject sql =
        executeJdbcRequest(
            String.format(
                "SELECT obj, b64, bytes64, emb_alias FROM %s WHERE doc_id = 1", STORED_INDEX));
    verifySchema(
        sql,
        schema("obj", "object"),
        schema("b64", "nested"),
        schema("bytes64", "nested"),
        schema("emb_alias", "nested"));
    assertEquals(
        asBefore,
        rawDatarows(
            executeQuery(
                String.format(
                    "SELECT obj, b64, bytes64, emb_alias FROM %s WHERE doc_id = 1", STORED_INDEX),
                "jdbc")));
    disableCalcite();
    try {
      String ppl =
          String.format(
              "source=%s | where doc_id = 1 | fields obj, b64, bytes64, emb_alias", STORED_INDEX);
      verifySchema(
          executeQuery(ppl),
          schema("obj", "struct"),
          schema("b64", "array"),
          schema("bytes64", "array"),
          schema("emb_alias", "array"));
      assertEquals(asBefore, rawDatarows(executeQueryToString(ppl)));
      assertEquals(
          "knn_vector",
          typeNameOf(executeQuery(String.format("describe %s", STORED_INDEX)), "emb_alias"));
    } finally {
      enableCalcite();
    }
    assertEquals(
        "vector",
        typeNameOf(executeQuery(String.format("describe %s", STORED_INDEX)), "emb_alias"));
  }

  /** A vector error is reported as it is even when Calcite errors may fall back to V2. */
  @Test
  public void testVectorErrorDoesNotFallBackToV2() throws IOException {
    withFallbackEnabled(
        () -> {
          ResponseException e =
              assertThrows(
                  ResponseException.class,
                  () -> executeQuery(String.format("source=%s | sort embedding", VECTOR_INDEX)));
          assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
          assertThat(
              e.getMessage(),
              containsString("VECTOR values cannot be compared, sorted or grouped"));
        },
        "a vector error must not fall back to V2");
  }

  /**
   * The {@code datarows} of a raw JSON response, without whitespace. Compared as text because
   * {@code rows()} matching treats 1 and 1.0 as equal.
   */
  private static String rawDatarows(String response) {
    int start = response.indexOf('[', response.indexOf("\"datarows\""));
    int depth = 0;
    for (int i = start; i < response.length(); i++) {
      char c = response.charAt(i);
      if (c == '[') {
        depth++;
      } else if (c == ']' && --depth == 0) {
        return response.substring(start, i + 1).replaceAll("\\s", "");
      }
    }
    throw new AssertionError("No datarows in " + response);
  }

  /** SQL and the V2 engine return the elements as stored, exactly as before the vector type. */
  @Test
  public void testSqlAndV2ReturnVectorElementsAsStored() throws IOException {
    String asStored = "[[1,[1,2,3]],[2,[-0.006929283495992422,0.123456789012,3.5]]]";
    assertEquals(
        asStored,
        rawDatarows(
            executeQuery(
                String.format("SELECT doc_id, embedding FROM %s ORDER BY doc_id", PRECISION_INDEX),
                "jdbc")));
    disableCalcite();
    try {
      assertEquals(
          asStored,
          rawDatarows(
              executeQueryToString(
                  String.format(
                      "source=%s | sort doc_id | fields doc_id, embedding", PRECISION_INDEX))));
    } finally {
      enableCalcite();
    }
  }

  /** The Calcite engine returns the vector column's float elements. */
  @Test
  public void testCalciteReturnsFloatElements() throws IOException {
    assertEquals(
        "[[1,[1.0,2.0,3.0]],[2,[-0.0069292835,0.12345679,3.5]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | sort doc_id | fields doc_id, embedding", PRECISION_INDEX))));
  }

  private static void createAndLoad(String index, String mapping, String bulk) throws IOException {
    if (!isIndexExist(client(), index)) {
      createIndexByRestClient(client(), index, mapping);
      Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
      request.setJsonEntity(bulk);
      performRequest(client(), request);
    }
  }

  private static boolean isKnnPluginInstalled() {
    try {
      Response response = client().performRequest(new Request("GET", "/_cat/plugins?h=component"));
      String body = new String(response.getEntity().getContent().readAllBytes());
      return body.contains("opensearch-knn");
    } catch (IOException e) {
      return false;
    }
  }

  @Test
  public void testProjectionReadsVectorsAndReportsVectorType() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort doc_id | fields doc_id, embedding, code", VECTOR_INDEX));
    verifySchema(
        result, schema("doc_id", "int"), schema("embedding", "vector"), schema("code", "vector"));
    verifyDataRowsInOrder(
        result,
        rows(1, List.of(1.0, 2.0, 3.0), List.of(1, -2)),
        rows(2, List.of(0.5, 0.25, -1.0), List.of(127, -128)),
        rows(3, null, null));
  }

  @Test
  public void testDefaultProjectionIncludesVectors() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | where doc_id = 1", VECTOR_INDEX));
    verifySchema(
        result, schema("doc_id", "int"), schema("embedding", "vector"), schema("code", "vector"));
  }

  @Test
  public void testJoinWithSecondIndexPassesVectorThrough() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | join left=v right=d on v.doc_id = d.doc_id %s"
                    + " | fields d.title, v.embedding",
                VECTOR_INDEX, DOCS_INDEX));
    verifySchema(result, schema("title", "string"), schema("embedding", "vector"));
    verifyDataRows(
        result, rows("first", List.of(1.0, 2.0, 3.0)), rows("second", List.of(0.5, 0.25, -1.0)));
  }

  @Test
  public void testFilterOnVectorNullness() throws IOException {
    verifyDataRows(
        executeQuery(
            String.format("source=%s | where isnotnull(embedding) | fields doc_id", VECTOR_INDEX)),
        rows(1),
        rows(2));
    verifyDataRows(
        executeQuery(
            String.format("source=%s | where isnull(embedding) | fields doc_id", VECTOR_INDEX)),
        rows(3));
  }

  @Test
  public void testFieldExcludedFromSourceReadsNull() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields doc_id, embedding, code", EXCLUDED_INDEX));
    verifySchema(
        result, schema("doc_id", "int"), schema("embedding", "vector"), schema("code", "vector"));
    verifyDataRows(result, rows(1, null, List.of(1, -2)));
  }

  @Test
  public void testV2EngineProjectsVectors() throws IOException {
    disableCalcite();
    try {
      JSONObject result =
          executeQuery(
              String.format(
                  "source=%s | where doc_id = 1 | fields doc_id, embedding", VECTOR_INDEX));
      // The V2 engine keeps the name knn_vector had before: array.
      verifySchema(result, schema("doc_id", "int"), schema("embedding", "array"));
      verifyDataRows(result, rows(1, List.of(1.0, 2.0, 3.0)));
      verifyDataRows(
          executeQuery(
              String.format(
                  "source=%s | where doc_id = 1 | eval t = typeof(embedding) | fields t",
                  VECTOR_INDEX)),
          rows("ARRAY"));
    } finally {
      enableCalcite();
    }
  }

  @Test
  public void testPplTypeofIsVector() throws IOException {
    verifyDataRows(
        executeQuery(
            String.format(
                "source=%s | where doc_id = 1 | eval t = typeof(embedding) | fields t",
                VECTOR_INDEX)),
        rows("VECTOR"));
  }

  @Test
  public void testDescribeNamesVectorInPplAndKnnVectorInSql() throws IOException {
    assertEquals(
        "vector",
        typeNameOf(executeQuery(String.format("describe %s", VECTOR_INDEX)), "embedding"));
    assertEquals(
        "knn_vector",
        typeNameOf(
            executeJdbcRequest(String.format("DESCRIBE TABLES LIKE %s", VECTOR_INDEX)),
            "embedding"));
    disableCalcite();
    try {
      // With the Calcite engine disabled, PPL describe reports what it did before: array.
      assertEquals(
          "array",
          typeNameOf(executeQuery(String.format("describe %s", VECTOR_INDEX)), "embedding"));
    } finally {
      enableCalcite();
    }
  }

  /** The TYPE_NAME of a column in a PPL describe or SQL DESCRIBE TABLES response. */
  private static String typeNameOf(JSONObject describe, String column) {
    JSONArray schema = describe.getJSONArray("schema");
    int columnName = -1;
    int typeName = -1;
    for (int i = 0; i < schema.length(); i++) {
      String name = schema.getJSONObject(i).getString("name");
      if (name.equals("COLUMN_NAME")) {
        columnName = i;
      } else if (name.equals("TYPE_NAME")) {
        typeName = i;
      }
    }
    JSONArray rows = describe.getJSONArray("datarows");
    for (int i = 0; i < rows.length(); i++) {
      if (column.equals(rows.getJSONArray(i).getString(columnName))) {
        return rows.getJSONArray(i).getString(typeName);
      }
    }
    throw new AssertionError("No column " + column + " in " + describe);
  }

  @Test
  public void testSqlProjectsVectors() {
    JSONObject result =
        executeJdbcRequest(
            String.format("SELECT doc_id, embedding FROM %s WHERE doc_id = 1", VECTOR_INDEX));
    // SQL keeps the name knn_vector had before: nested.
    verifySchema(result, schema("doc_id", "integer"), schema("embedding", "nested"));
    verifyDataRows(result, rows(1, List.of(1.0, 2.0, 3.0)));
    verifyDataRows(
        executeJdbcRequest(
            String.format("SELECT typeof(embedding) FROM %s WHERE doc_id = 1", VECTOR_INDEX)),
        rows("NESTED"));
  }

  @Test
  public void testSqlVectorSearchColumnTypeIsUnchanged() {
    JSONObject result =
        executeJdbcRequest(
            String.format(
                "SELECT v.doc_id, v.embedding FROM vectorSearch(table='%s', field='embedding',"
                    + " vector='[1.0, 2.0, 3.0]', option='k=1') AS v LIMIT 1",
                VECTOR_INDEX));
    verifySchema(result, schema("doc_id", "integer"), schema("embedding", "nested"));
    verifyDataRows(result, rows(1, List.of(1.0, 2.0, 3.0)));
  }

  /** A filter over vector elements runs as a pushed script, which reads typed elements. */
  @Test
  public void testScriptFilterOverVectorElements() throws IOException {
    verifyDataRows(
        executeQuery(
            String.format(
                "source=%s | where mvindex(embedding, 0) > 0.75 | fields doc_id", VECTOR_INDEX)),
        rows(1));
    verifyDataRows(
        executeQuery(
            String.format(
                "source=%s | where mvindex(code, 0) > 100 | fields doc_id", VECTOR_INDEX)),
        rows(2));
  }

  @Test
  public void testScriptFilterOverVectorElementsIsPushedDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String explain =
        explainQueryToString(
            String.format(
                "source=%s | where mvindex(embedding, 0) > 0.75 | fields doc_id", VECTOR_INDEX));
    assertThat(explain, containsString("script"));
  }

  /** Pushdown and in-memory evaluation agree on a vector excluded from _source: it is null. */
  @Test
  public void testNullTestsOnVectorExcludedFromSource() throws IOException {
    verifyDataRows(
        executeQuery(
            String.format("source=%s | where isnull(embedding) | fields doc_id", EXCLUDED_INDEX)),
        rows(1));
    verifyNumOfRows(
        executeQuery(
            String.format(
                "source=%s | where isnotnull(embedding) | fields doc_id", EXCLUDED_INDEX)),
        0);
  }

  @Test
  public void testSortAndGroupByOnVectorAreRejected() {
    for (String command :
        List.of(
            "sort embedding",
            "stats count() by embedding",
            "dedup code",
            "stats min(embedding)",
            "stats dc(code)",
            "join embedding " + VECTOR_INDEX,
            "lookup " + VECTOR_INDEX + " embedding",
            // A join without keys would compare the vector columns both inputs share.
            "join " + VECTOR_INDEX,
            "xyseries embedding doc_id in ('1') doc_id",
            "graphLookup " + VECTOR_INDEX + " start=embedding edge=doc_id-->doc_id as path")) {
      ResponseException e =
          assertThrows(
              ResponseException.class,
              () -> executeQuery(String.format("source=%s | %s", VECTOR_INDEX, command)));
      assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
      assertThat(
          e.getMessage(), containsString("VECTOR values cannot be compared, sorted or grouped"));
    }
  }

  @Test
  public void testCompareVectorIsRejected() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format("source=%s | where embedding = embedding", VECTOR_INDEX)));
    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    assertThat(
        e.getMessage(), containsString("VECTOR values cannot be compared, sorted or grouped"));
  }

  /**
   * A column is converted by its type, not by its name: an aggregation output or an eval column
   * named like a vector field, or like an object holding one, keeps its value.
   */
  @Test
  public void testColumnsNamedLikeVectorFieldsKeepTheirValues() throws IOException {
    JSONObject count =
        executeQuery(String.format("source=%s | stats count() as embedding", VECTOR_INDEX));
    verifySchema(count, schema("embedding", "bigint"));
    verifyDataRows(count, rows(3));
    verifyDataRows(
        executeQuery(String.format("source=%s | stats count() as obj", STORED_INDEX)), rows(3));
    JSONObject eval =
        executeQuery(
            String.format(
                "source=%s | where doc_id = 1 | eval embedding = 1 | fields embedding",
                VECTOR_INDEX));
    verifySchema(eval, schema("embedding", "int"));
    verifyDataRows(eval, rows(1));
  }

  /**
   * Across a float index and a byte index, the elements are floats and a base64 value is decoded
   * with the data type of its own index: as float32 values from the float index, as bytes from the
   * byte index.
   */
  @Test
  public void testMultiIndexBase64IsDecodedWithItsIndexDataType() throws IOException {
    String query =
        String.format(
            "source=%s,%s | sort doc_id | fields doc_id, v", MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX);
    verifySchema(executeQuery(query), schema("doc_id", "int"), schema("v", "vector"));
    assertEquals(
        "[[1,[1.0,-2.5,2.0,3.0]],[2,[1.0,2.0,3.0,-1.0]],[3,[0.5,1.0,2.0,3.0]]]",
        rawDatarows(executeQueryToString(query)));
  }

  /**
   * A script pushed down to OpenSearch cannot tell a document's index, so it fails on such a base64
   * value rather than decode it with the wrong data type; evaluated by the Calcite engine, the
   * filter decodes it with its index's data type.
   */
  @Test
  public void testMultiIndexBase64InAPushedDownScriptFails() throws IOException {
    String query =
        String.format(
            "source=%s,%s | where mvindex(v, 0) > 0.75 | sort doc_id | fields doc_id",
            MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX);
    if (isPushdownDisabled()) {
      verifyDataRowsInOrder(executeQuery(query), rows(1), rows(2));
      return;
    }
    ResponseException e = assertThrows(ResponseException.class, () -> executeQuery(query));
    assertThat(e.getMessage(), containsString("cannot decode the base64 vector of [v]"));
  }

  /**
   * Three indices map field f as knn_vector of two dimensions and as nested with a sub-field: the
   * merge keeps the sub-field and the query runs.
   */
  @Test
  public void testMultiIndexMergeWithNestedField() throws IOException {
    JSONObject describe = executeQuery(String.format("describe %s", MERGE_INDICES));
    assertEquals("int", typeNameOf(describe, "f.sub"));
    verifyDataRows(
        executeQuery(String.format("source=%s | fields doc_id", MERGE_INDICES)),
        rows(1),
        rows(1),
        rows(1));
  }

  /**
   * first, last and dedup over float and byte vectors, JSON arrays and base64, give the whole
   * vector with pushdown on (a top_hits reads the vector from _source) and off.
   */
  @Test
  public void testFirstLastAndDedupReturnWholeVectors() throws IOException {
    assertEquals(
        "[[[1.0,2.5,3.0],[1,-2,3],[1.0,-2.5,2.0],[1,2,-1]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | stats first(f), first(b), last(f), last(b)", TOP_HITS_INDEX))));
    assertEquals(
        "[[[1.0,2.5,3.0],[1,-2,3],\"a\"],[[1.0,-2.5,2.0],[1,2,-1],\"b\"]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | stats first(f), first(b) by g | sort g", TOP_HITS_INDEX))));
    assertEquals(
        "[[1,[1.0,2.5,3.0],[1,-2,3]],[2,[1.0,-2.5,2.0],[1,2,-1]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | dedup g | sort doc_id | fields doc_id, f, b", TOP_HITS_INDEX))));
    if (!isPushdownDisabled()) {
      String explain =
          explainQueryToString(String.format("source=%s | stats first(f)", TOP_HITS_INDEX));
      assertThat(explain, containsString("top_hits"));
      assertThat(explain, containsString("includes"));
    }
  }

  /**
   * The hit of a routed document has its _routing among its fields; first, last, earliest and
   * latest of a vector read the vector from _source alone, not that value, and so does a dedup.
   */
  @Test
  public void testVectorsOfARoutedDocument() throws IOException {
    // The top_hits hit of the document carries _routing.
    Request search = new Request("POST", "/" + ROUTED_INDEX + "/_search");
    search.setJsonEntity(
        "{\"size\": 0, \"aggs\": {\"t\": {\"top_hits\": {\"size\": 1,"
            + " \"_source\": {\"includes\": [\"f\"]}}}}}");
    JSONObject hit =
        new JSONObject(getResponseBody(client().performRequest(search)))
            .getJSONObject("aggregations")
            .getJSONObject("t")
            .getJSONObject("hits")
            .getJSONArray("hits")
            .getJSONObject(0);
    assertEquals("r1", hit.getString("_routing"));
    assertEquals(
        "[[[1.0,2.5,3.0],[1.0,2.5,3.0],[1.0,2.5,3.0],[1.0,2.5,3.0],[4.0,5.0,6.0],[4.0,5.0,6.0]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | stats first(f), last(f), earliest(f, ts), latest(f, ts),"
                        + " first(obj.vec), earliest(obj.vec, ts)",
                    ROUTED_INDEX))));
    assertEquals(
        "[[\"a\",[1.0,2.5,3.0]]]",
        rawDatarows(
            executeQueryToString(
                String.format("source=%s | dedup g | fields g, f", ROUTED_INDEX))));
  }

  /** take does not accept a vector. */
  @Test
  public void testTakeOfVectorIsRejected() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeQuery(String.format("source=%s | stats take(f, 2)", TOP_HITS_INDEX)));
    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    assertThat(e.getMessage(), containsString("but got [VECTOR,INTEGER]"));
  }

  /**
   * Across a float and a byte index, first and dedup decode each base64 value with its own index's
   * data type, with pushdown on (the top_hits rows record their hit's index) and off.
   */
  @Test
  public void testFirstAndDedupAcrossMixedEncodingIndices() throws IOException {
    String vectors = "[1.0,-2.5,2.0,3.0],1],[[1.0,2.0,3.0,-1.0],2],[[0.5,1.0,2.0,3.0],3]]";
    assertEquals(
        "[[" + vectors,
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s,%s | stats first(v) by doc_id | sort doc_id",
                    MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX))));
    assertEquals(
        "[[1,[1.0,-2.5,2.0,3.0]],[2,[1.0,2.0,3.0,-1.0]],[3,[0.5,1.0,2.0,3.0]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s,%s | dedup doc_id | sort doc_id | fields doc_id, v",
                    MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX))));
    if (!isPushdownDisabled()) {
      assertThat(
          explainQueryToString(
              String.format(
                  "source=%s,%s | stats first(v) by doc_id", MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX)),
          containsString("top_hits"));
      assertThat(
          explainQueryToString(
              String.format(
                  "source=%s,%s | dedup doc_id | fields doc_id, v",
                  MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX)),
          containsString("top_hits"));
    }
  }

  /**
   * A dedup over an index with an alias to a vector is pushed down and returns both the alias and
   * its target, which the plan reads as one field.
   */
  @Test
  public void testDedupWithAliasToVector() throws IOException {
    String query =
        String.format(
            "source=%s | dedup doc_id | sort doc_id | fields doc_id, embedding, emb_alias",
            STORED_INDEX);
    assertEquals(
        "[[1,[1.0,2.0,3.0],[1.0,2.0,3.0]],[2,null,null],[3,null,null]]",
        rawDatarows(executeQueryToString(query)));
    if (!isPushdownDisabled()) {
      assertThat(explainQueryToString(query), containsString("top_hits"));
    }
  }

  /**
   * earliest and latest over float and byte vectors, JSON arrays and base64, give the whole vector
   * with pushdown on (a top_hits that reads the vector from _source) and off.
   */
  @Test
  public void testEarliestAndLatestReturnWholeVectors() throws IOException {
    String query =
        String.format(
            "source=%s | stats earliest(f, ts), latest(f, ts), earliest(b, ts), latest(b, ts)",
            TOP_HITS_INDEX);
    assertEquals(
        "[[[1.0,2.5,3.0],[1.0,-2.5,2.0],[1,-2,3],[1,2,-1]]]",
        rawDatarows(executeQueryToString(query)));
    if (!isPushdownDisabled()) {
      assertThat(explainQueryToString(query), containsString("top_hits"));
    }
  }

  /**
   * On an index with k-NN derived source, first, last, earliest, latest, dedup and an object column
   * return the injected vectors, with pushdown on and off.
   */
  @Test
  public void testDerivedSourceVectors() throws IOException {
    assertEquals(
        "[[[1.0,2.5,3.0],[0.5,1.0,2.0],[1,-2,3],[1,2,-1],[4.0,5.0,6.0],[7.0,8.0,9.0]]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | stats first(f), last(f), earliest(b, ts), latest(b, ts),"
                        + " first(obj.vec), latest(obj.vec, ts)",
                    DERIVED_INDEX))));
    // A projected object names its vector in the _source includes, so k-NN injects it.
    assertEquals(
        "[[1,{\"vec\":[4.0,5.0,6.0],\"name\":\"x\"}],[2,{\"vec\":[7.0,8.0,9.0],\"name\":\"y\"}]]",
        rawDatarows(
            executeQueryToString(
                String.format("source=%s | sort doc_id | fields doc_id, obj", DERIVED_INDEX))));
    assertEquals(
        "[[1,[1.0,2.5,3.0],{\"vec\":[4.0,5.0,6.0],\"name\":\"x\"}],"
            + "[2,[0.5,1.0,2.0],{\"vec\":[7.0,8.0,9.0],\"name\":\"y\"}]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | dedup g | sort doc_id | fields doc_id, f, obj", DERIVED_INDEX))));
  }

  /** Across a float and a byte index, a dedup decodes a vector inside an object per index. */
  @Test
  public void testDedupOfObjectsAcrossMixedEncodingIndices() throws IOException {
    String query =
        String.format(
            "source=%s,%s | fields doc_id, obj | dedup doc_id | sort doc_id",
            MIXED_FLOAT_INDEX, MIXED_BYTE_INDEX);
    assertEquals(
        "[[1,{\"vec\":[1.0,-2.5,2.0,3.0]}],[2,{\"vec\":[1.0,2.0,3.0,-1.0]}],[3,null]]",
        rawDatarows(executeQueryToString(query)));
    if (!isPushdownDisabled()) {
      assertThat(explainQueryToString(query), containsString("top_hits"));
    }
  }

  /** The V2 engine rejects take over a vector, as it rejected it over the ARRAY of before. */
  @Test
  public void testV2TakeOfVectorIsRejected() throws IOException {
    disableCalcite();
    try {
      ResponseException e =
          assertThrows(
              ResponseException.class,
              () -> executeQuery(String.format("source=%s | stats take(f, 2)", TOP_HITS_INDEX)));
      assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
      assertThat(e.getMessage(), containsString("but got [ARRAY,INTEGER]"));
    } finally {
      enableCalcite();
    }
  }

  /**
   * A dedup with a script column asks for _source, which a hit has no other way when the top_hits
   * has script fields, and reads the vectors from it.
   */
  @Test
  public void testDedupWithAScriptColumnReadsVectors() throws IOException {
    assertEquals(
        "[[1,[1.0,2.5,3.0],[1,-2,3],1],[2,[1.0,-2.5,2.0],[1,2,-1],1]]",
        rawDatarows(
            executeQueryToString(
                String.format(
                    "source=%s | eval x = 1 | dedup g | sort doc_id | fields doc_id, f, b, x",
                    TOP_HITS_INDEX))));
  }
}
