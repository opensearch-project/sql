/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.opensearch.storage.serde.ExtendedRelJson;

/**
 * Security regression test for the {@code opensearch_compounded_script} Calcite script path, where
 * a client-supplied {@code _search} script deserializes a JSON {@code RexNode} and Janino-compiles
 * Java generated from it. Each test submits an attacker-controlled RexNode shape whose payload
 * would write a marker file from a {@code static { }} block at class-load time, and asserts the
 * file was not created. The side effect is non-throwing so a vulnerable node is not crashed by an
 * {@code ExceptionInInitializerError}.
 */
public class CalciteScriptEngineInjectionIT extends SQLIntegTestCase {

  private static final String INDEX = "calcite_script_rce_it";

  @Override
  protected void init() throws Exception {
    // setUpIndices() runs per test method; the index may already exist from an earlier one.
    Request exists = new Request("HEAD", "/" + INDEX);
    if (client().performRequest(exists).getStatusLine().getStatusCode() == 200) {
      return;
    }
    Request createIndex = new Request("PUT", "/" + INDEX);
    createIndex.setJsonEntity("{\"mappings\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}");
    client().performRequest(createIndex);
    Request indexDoc = new Request("POST", "/" + INDEX + "/_doc?refresh=true");
    indexDoc.setJsonEntity("{\"name\":\"alice\"}");
    client().performRequest(indexDoc);
  }

  /**
   * A struct-typed literal whose element is a list. Calcite emits it as {@code
   * Arrays.asList('<content>')} using a Java char literal, whose writer does not escape backslashes
   * — so JLS backslash-u escapes terminate the literal early. Rejected by {@code
   * RexLiteralSafetyValidator}.
   */
  @Test
  public void calciteScriptEngineMustNotExecuteInjectedJava() throws Exception {
    runInjectionProbe("struct_list", CalciteScriptEngineInjectionIT::buildStructInjectionBody);
  }

  /**
   * A SARG literal with an attacker-controlled string bound. {@code RexLiteralSafetyValidator}
   * allows SARG through unchecked, relying on Calcite emitting its bounds into backslash-escaped
   * Java string literals. This asserts that invariant end-to-end.
   */
  @Test
  public void calciteScriptEngineMustNotExecuteInjectedJavaViaSarg() throws Exception {
    runInjectionProbe("sarg", CalciteScriptEngineInjectionIT::buildSargInjectionBody);
  }

  private void runInjectionProbe(String tag, PayloadBuilder payloadBuilder) throws Exception {
    // A path both the node JVM and this test JVM can reach (same host/user; SM disabled in IT).
    Path dir =
        Paths.get(
            System.getProperty("project.root", System.getProperty("java.io.tmpdir")), "build");
    Files.createDirectories(dir);
    Path markerFile = dir.resolve("calcite_rce_proof_" + tag + "_" + randomAlphaOfLength(12));
    Files.deleteIfExists(markerFile);

    try {
      Request search = new Request("POST", "/" + INDEX + "/_search");
      search.addParameter("error_trace", "true");
      search.setJsonEntity(payloadBuilder.build(markerFile.toAbsolutePath().toString()));

      try {
        client().performRequest(search);
      } catch (ResponseException expected) {
        // The script errors either way; we assert on the side effect, not the response status.
      }

      assertFalse(
          "Injected Java executed during "
              + tag
              + " script compilation (RCE): the static initializer created "
              + markerFile,
          Files.exists(markerFile));
    } finally {
      Files.deleteIfExists(markerFile);
    }
  }

  @FunctionalInterface
  private interface PayloadBuilder {
    String build(String markerPath) throws Exception;
  }

  /**
   * String content that escapes its enclosing Java literal and injects a {@code static { }} block
   * writing {@code markerPath}, assuming the emitter does not escape backslashes. Built without
   * Java unicode escapes in this source so that javac does not resolve them here.
   */
  private static String injectionPayload(String markerPath) {
    final String backslash = String.valueOf((char) 92);
    final String sq = backslash + "u0027"; // Janino pre-pass -> '
    final String qq = backslash + "u0022"; // Janino pre-pass -> "
    return "A"
        + sq
        + ")}; } static { try { new java.io.FileOutputStream("
        + qq
        + markerPath
        + qq
        + ").close(); } catch (Throwable t) {} } public Object[] _z(Object _r){ return new"
        + " Object[]{ java.util.Arrays.asList("
        + sq
        + "B";
  }

  private static String buildStructInjectionBody(String markerPath) throws Exception {
    JSONObject field = new JSONObject().put("name", "f").put("type", "ANY").put("nullable", false);
    JSONObject type =
        new JSONObject().put("fields", new JSONArray().put(field)).put("nullable", false);
    JSONObject literal =
        new JSONObject()
            .put("literal", new JSONArray().put(injectionPayload(markerPath)))
            .put("type", type);

    return wrapAsCompoundedScriptBody(literal.toString());
  }

  /** Builds {@code SEARCH(?0 : CHAR, Sarg[singleton(<payload>)])}. */
  private static String buildSargInjectionBody(String markerPath) throws Exception {
    String content = injectionPayload(markerPath);
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, content.length());
    // Route through makeLiteral so Calcite attaches a valid charset/collation to the NlsString.
    RexNode charBound = rexBuilder.makeLiteral(content, charType, true);
    NlsString nlsInjection = (NlsString) ((RexLiteral) charBound).getValue();
    Sarg<NlsString> sarg =
        Sarg.of(
            RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<NlsString>builder().add(Range.singleton(nlsInjection)).build());
    RexNode sargLiteral = rexBuilder.makeSearchArgumentLiteral(sarg, charType);
    RexNode call =
        rexBuilder.makeCall(
            SqlStdOperatorTable.SEARCH, rexBuilder.makeDynamicParam(charType, 0), sargLiteral);

    // Skip RexStandardizer, which would expand the SEARCH into comparisons over parameters. A
    // hand-forged payload never runs it, so the SARG reaches deserialize() still inlined.
    JsonBuilder jsonBuilder = new JsonBuilder();
    RelJson relJson = ExtendedRelJson.create(jsonBuilder);
    return wrapAsCompoundedScriptBody(jsonBuilder.toJsonString(relJson.toJson(call)));
  }

  /** {@code deserialize()} expects base64(ObjectOutputStream.writeObject(relJson)). */
  private static String wrapAsCompoundedScriptBody(String relJsonString) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(relJsonString);
    }
    String encodedScript = Base64.getEncoder().encodeToString(bytes.toByteArray());

    String source =
        new JSONObject().put("langType", "calcite").put("script", encodedScript).toString();
    JSONObject scriptQuery =
        new JSONObject().put("lang", "opensearch_compounded_script").put("source", source);
    return new JSONObject()
        .put(
            "query",
            new JSONObject()
                .put(
                    "bool",
                    new JSONObject()
                        .put(
                            "filter",
                            new JSONObject()
                                .put("script", new JSONObject().put("script", scriptQuery)))))
        .toString();
  }
}
