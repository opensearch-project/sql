/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TELEMETRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteEvalCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.TELEMETRY);

    // Create test data for string concatenation
    Request request1 = new Request("PUT", "/test_eval/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"Alice\", \"age\": 25, \"title\": \"Engineer\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_eval/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"Bob\", \"age\": 30, \"title\": \"Manager\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_eval/_doc/3?refresh=true");
    request3.setJsonEntity("{\"name\": \"Charlie\", \"age\": null, \"title\": \"Analyst\"}");
    client().performRequest(request3);

    // Index with a struct field `agent` to reproduce the reviewer's case from PR #5351:
    //   source=<idx with agent struct> | fields agent | eval agent.name = "test"
    // Rely on dynamic mapping — OpenSearch infers `agent` as an object with string children
    // from the document contents. Using dynamic mapping keeps the init idempotent across
    // repeated `@Before` invocations in the preserved cluster.
    Request agentDoc1 = new Request("PUT", "/test_eval_agent/_doc/1?refresh=true");
    agentDoc1.setJsonEntity(
        "{\"agent\": {\"name\": \"winlogbeat\", \"version\": \"7.0\"}, \"message\": \"hello\"}");
    client().performRequest(agentDoc1);

    Request agentDoc2 = new Request("PUT", "/test_eval_agent/_doc/2?refresh=true");
    agentDoc2.setJsonEntity(
        "{\"agent\": {\"name\": \"filebeat\", \"version\": \"8.1\"}, \"message\": \"world\"}");
    client().performRequest(agentDoc2);
  }

  @Test
  public void testEvalStringConcatenation() throws IOException {
    JSONObject result = executeQuery("source=test_eval | eval greeting = 'Hello ' + name");
    verifySchema(
        result,
        schema("name", "string"),
        schema("title", "string"),
        schema("age", "bigint"),
        schema("greeting", "string"));
    verifyDataRows(
        result,
        rows("Alice", "Engineer", 25, "Hello Alice"),
        rows("Bob", "Manager", 30, "Hello Bob"),
        rows("Charlie", "Analyst", null, "Hello Charlie"));
  }

  @Test
  public void testEvalStringConcatenationWithNullField() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | eval age_desc = 'Age: ' + CAST(age AS STRING) | fields name, age,"
                + " age_desc");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("age_desc", "string"));
    verifyDataRows(
        result,
        rows("Alice", 25, "Age: 25"),
        rows("Bob", 30, "Age: 30"),
        rows("Charlie", null, null));
  }

  @Test
  public void testEvalStringConcatenationWithLiterals() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | eval full_info = 'Name: ' + name + ', Title: ' + title | fields"
                + " name, title, full_info");
    verifySchema(
        result, schema("name", "string"), schema("title", "string"), schema("full_info", "string"));
    verifyDataRows(
        result,
        rows("Alice", "Engineer", "Name: Alice, Title: Engineer"),
        rows("Bob", "Manager", "Name: Bob, Title: Manager"),
        rows("Charlie", "Analyst", "Name: Charlie, Title: Analyst"));
  }

  @Test
  public void testEvalDottedNameDoesNotDropStructParent() throws IOException {
    // Reviewer's case from PR #5351: assigning a new dotted-path column must not remove the
    // struct-parent column that happens to be a prefix of the eval target.
    // Equivalent SPL1 query:
    //   source=<idx with agent struct> | fields agent | eval agent.name = "test"
    // Before the fix, the prefix-override in shouldOverrideField silently dropped the `agent`
    // column entirely from the result schema. With the fix, `agent` is preserved.
    // The newly-created literal column `agent.name` is also available (verified via an
    // explicit trailing `fields` projection that bypasses tryToRemoveNestedFields).
    JSONObject result =
        executeQuery(
            "source=test_eval_agent | fields agent | eval `agent.name` = 'test' | fields agent,"
                + " `agent.name`");
    verifySchema(result, schema("agent", "struct"), schema("agent.name", "string"));
    verifyDataRows(
        result,
        rows(ImmutableMap.of("name", "winlogbeat", "version", "7.0"), "test"),
        rows(ImmutableMap.of("name", "filebeat", "version", "8.1"), "test"));
  }

  @Test
  public void testEvalDottedNamePreservesStructParent_ImplicitProject() throws IOException {
    // Complementary coverage for the reviewer's case without the explicit trailing projection.
    // With the implicit `fields *` (AllFields) that the PPL parser appends, the downstream
    // `tryToRemoveNestedFields` pass still collapses the flattened leaf back into its struct
    // parent -- but the important regression guard is that the struct parent `agent` is no
    // longer dropped by `shouldOverrideField`'s prefix branch.
    JSONObject result =
        executeQuery("source=test_eval_agent | fields agent | eval `agent.name` = 'test'");
    verifySchema(result, schema("agent", "struct"));
  }

  @Test
  public void testEvalOverrideOfFlattenedNestedLeafSurvivesImplicitProject() throws IOException {
    // Regression guard for PR #5351: eval assigning a new value to a dotted name that matches an
    // existing OpenSearch flattened nested leaf must not have that value silently eaten by the
    // implicit `fields *` pass.
    //
    // The telemetry mapping exposes struct parents (resource, resource.attributes, ...,
    // resource.attributes.telemetry.sdk) side-by-side with the flattened leaves. When eval
    // overrides the leaf, projectPlusOverriding now prunes those struct parents so a subsequent
    // tryToRemoveNestedFields pass does not delete the overridden leaf on the way out.
    //
    // Before the fix, this query returned rows with the original `resource` struct (still
    // containing the pre-override integer version) and no `resource.attributes.telemetry.sdk.*`
    // flattened leaves at all -- the "OVERRIDE" string was completely lost.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval `resource.attributes.telemetry.sdk.version` = 'OVERRIDE'",
                TEST_INDEX_TELEMETRY));

    verifyDataRows(
        result,
        rows(true, "java", "opentelemetry", 9, "OVERRIDE"),
        rows(false, "python", "opentelemetry", 12, "OVERRIDE"),
        rows(true, "javascript", "opentelemetry", 9, "OVERRIDE"),
        rows(false, "go", "opentelemetry", 16, "OVERRIDE"),
        rows(true, "rust", "opentelemetry", 12, "OVERRIDE"));
  }

  @Test
  public void testEvalOverrideOfFlattenedNestedLeafWithExplicitProject() throws IOException {
    // Control for the test above: with an explicit trailing `fields` projection, the implicit
    // `fields *` codepath (and tryToRemoveNestedFields) does not run, so eval always returned
    // the overridden value even before the fix. This test pins the user-facing contract for the
    // explicit-projection variant regardless of internal pruning behaviour.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval `resource.attributes.telemetry.sdk.version` = 'OVERRIDE' | fields"
                    + " `resource.attributes.telemetry.sdk.version`",
                TEST_INDEX_TELEMETRY));
    verifySchema(result, schema("resource.attributes.telemetry.sdk.version", "string"));
    verifyDataRows(
        result,
        rows("OVERRIDE"),
        rows("OVERRIDE"),
        rows("OVERRIDE"),
        rows("OVERRIDE"),
        rows("OVERRIDE"));
  }

  @Test
  public void testEvalStringConcatenationWithExistingData() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval full_name = firstname + ' ' + lastname | head 3 | fields"
                    + " firstname, lastname, full_name",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("full_name", "string"));
    verifyDataRows(
        result,
        rows("Amber JOHnny", "Duke Willmington", "Amber JOHnny Duke Willmington"),
        rows("Hattie", "Bond", "Hattie Bond"),
        rows("Nanette", "Bates", "Nanette Bates"));
  }
}
