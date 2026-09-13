/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLPatternsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.WEBLOG);
    loadIndex(Index.HDFS_LOGS);
  }

  @Test
  public void testSimplePatternLabelMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label | sort account_number | head 1 | fields"
                    + " email, patterns_field",
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("amberduke@pyrami.com", "<*>@<*>.<*>"));
  }

  @Test
  public void testSimplePatternLabelMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label show_numbered_token=true | sort"
                    + " account_number | head 1 | fields email, patterns_field, tokens",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("email", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "amberduke@pyrami.com",
            "<token1>@<token2>.<token3>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("amberduke"),
                "<token2>",
                ImmutableList.of("pyrami"),
                "<token3>",
                ImmutableList.of("com"))));
  }

  @Test
  public void testSimplePatternLabelMode_NullFieldReturnEmpty() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns message | where isnull(message) | fields message,"
                    + " patterns_field",
                TEST_INDEX_WEBLOGS));
    verifySchema(result, schema("message", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows(null, ""), rows(null, ""));
  }

  @Test
  public void testSimplePatternLabelMode_EmptyStringReturnEmpty() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval message2 = '' | head 1 | patterns message2 | fields message2,"
                    + " patterns_field",
                TEST_INDEX_WEBLOGS));
    verifySchema(result, schema("message2", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("", ""));
  }

  @Test
  public void testSimplePatternLabelModeWithCustomPattern_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label show_numbered_token=true pattern='@.*' |"
                    + " sort account_number | head 1 | fields email, patterns_field, tokens",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("email", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "amberduke@pyrami.com",
            "amberduke<token1>",
            ImmutableMap.of("<token1>", ImmutableList.of("@pyrami.com"))));
  }

  @Test
  public void testSimplePatternAggregationMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email mode=aggregation max_sample_count=3", TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("pattern_count", "bigint"),
        schema("patterns_field", "string"),
        schema("sample_logs", "array"));
    // The single pattern and its total count are shard-invariant; which max_sample_count emails
    // land in the sample is not, so assert the count exactly and that each sampled log is a valid
    // instance of the pattern (an email) rather than pinning the specific three.
    List<List<Object>> rows = dataRows(result);
    assertEquals(1, rows.size());
    List<Object> row = rows.get(0);
    assertEquals("<*>@<*>.<*>", row.get(0));
    assertEquals(7L, asLong(row.get(1)));
    List<String> samples = asStringList(row.get(2));
    assertEquals(3, samples.size());
    for (String s : samples) {
      assertTrue("not an email: " + s, EMAIL.matcher(s).matches());
    }
  }

  @Test
  public void testSimplePatternAggregationMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email mode=aggregation max_sample_count=3"
                    + " show_numbered_token=true",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("pattern_count", "bigint"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    // Pattern, count and token layout are shard-invariant; the sampled emails are not. Assert the
    // count and that, for every sampled email, the captured tokens reconstruct it exactly
    // (<token1>@<token2>.<token3>) so token/pattern correctness is verified without pinning which
    // three emails were sampled.
    List<List<Object>> rows = dataRows(result);
    assertEquals(1, rows.size());
    List<Object> row = rows.get(0);
    assertEquals("<token1>@<token2>.<token3>", row.get(0));
    assertEquals(7L, asLong(row.get(1)));
    Map<String, List<String>> tokens = tokenMap(row.get(2));
    assertEquals(Set.of("<token1>", "<token2>", "<token3>"), tokens.keySet());
    List<String> samples = asStringList(row.get(3));
    assertEquals(3, samples.size());
    List<String> t1 = tokens.get("<token1>");
    List<String> t2 = tokens.get("<token2>");
    List<String> t3 = tokens.get("<token3>");
    assertEquals(3, t1.size());
    assertEquals(3, t2.size());
    assertEquals(3, t3.size());
    for (int i = 0; i < samples.size(); i++) {
      assertEquals(samples.get(i), t1.get(i) + "@" + t2.get(i) + "." + t3.get(i));
      assertTrue("not an email: " + samples.get(i), EMAIL.matcher(samples.get(i)).matches());
    }
  }

  @Test
  public void testSimplePatternAggregationMode_WithGroupBy_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email by male mode=aggregation max_sample_count=1"
                    + " show_numbered_token=true",
                TEST_INDEX_BANK));
    verifySchemaInOrder(
        result,
        schema("male", "boolean"),
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    // The two groups and their counts (male=false:3, male=true:4) are shard-invariant; the single
    // sampled email per group is not. Assert the per-group count and that the sampled email is
    // reconstructed by its captured tokens.
    List<List<Object>> rows = dataRows(result);
    assertEquals(2, rows.size());
    Map<Object, Long> countsByMale = new HashMap<>();
    for (List<Object> row : rows) {
      Object male = row.get(0);
      assertEquals("<token1>@<token2>.<token3>", row.get(1));
      countsByMale.put(male, asLong(row.get(2)));
      Map<String, List<String>> tokens = tokenMap(row.get(3));
      assertEquals(Set.of("<token1>", "<token2>", "<token3>"), tokens.keySet());
      List<String> samples = asStringList(row.get(4));
      assertEquals(1, samples.size());
      List<String> t1 = tokens.get("<token1>");
      List<String> t2 = tokens.get("<token2>");
      List<String> t3 = tokens.get("<token3>");
      assertEquals(1, t1.size());
      assertEquals(1, t2.size());
      assertEquals(1, t3.size());
      assertEquals(samples.get(0), t1.get(0) + "@" + t2.get(0) + "." + t3.get(0));
      assertTrue("not an email: " + samples.get(0), EMAIL.matcher(samples.get(0)).matches());
    }
    assertEquals(Map.of(false, 3L, true, 4L), countsByMale);
  }

  @Test
  public void testBrainLabelMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=BRAIN mode=label"
                    + " max_sample_count=5 variable_count_threshold=5"
                    + " frequency_threshold_percentage=0.2 | sort pid | head 2 | fields content,"
                    + " patterns_field",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(result, schema("content", "string"), schema("patterns_field", "string"));
    // `head 2` has no stable order across shards; sort on the unique pid first so the two returned
    // documents are deterministic (pid 26 then pid 31). Pattern labeling is computed over the full
    // input, so the sort only pins which two labeled rows surface.
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
                + " blk_-3249711809227781266 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*>"
                + " size <*>"),
        rows(
            "PacketResponder failed for blk_-1547954353065580372",
            "PacketResponder failed <*> blk_<*>"));
  }

  @Test
  public void testBrainLabelMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=BRAIN mode=label"
                    + " max_sample_count=5 show_numbered_token=true variable_count_threshold=5"
                    + " frequency_threshold_percentage=0.2 | sort pid | head 2 | fields content,"
                    + " patterns_field, tokens",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("content", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    // See testBrainLabelMode_NotShowNumberedToken: sort pid pins the two returned documents
    // (pid 26 then pid 31) deterministically across shards.
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
                + " blk_-3249711809227781266 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2>"
                + " size <token3>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("10.251.107.19:50010"),
                "<token2>",
                ImmutableList.of("-3249711809227781266"),
                "<token3>",
                ImmutableList.of("67108864"))),
        rows(
            "PacketResponder failed for blk_-1547954353065580372",
            "PacketResponder failed <token1> blk_<token2>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("for"),
                "<token2>",
                ImmutableList.of("-1547954353065580372"))));
  }

  @Test
  public void testBrainAggregationMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=brain mode=aggregation"
                    + " variable_count_threshold=5",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("sample_logs", "array"));
    // Each pattern's count is 2 and its sample_logs covers both matching documents; only the order
    // within sample_logs (and the order of the pattern rows) varies across shards. Assert the four
    // patterns, their counts, and their full sample sets order-insensitively.
    Map<String, List<String>> expectedSamples = new HashMap<>();
    expectedSamples.put(PAT_VERIF, Arrays.asList(LOG_VERIF_4, LOG_VERIF_6));
    expectedSamples.put(PAT_ADDBLK, Arrays.asList(LOG_ADDBLK_1, LOG_ADDBLK_3));
    expectedSamples.put(PAT_ALLOC, Arrays.asList(LOG_ALLOC_2, LOG_ALLOC_5));
    expectedSamples.put(PAT_PACKET, Arrays.asList(LOG_PACKET_7, LOG_PACKET_8));

    List<List<Object>> rows = dataRows(result);
    assertEquals(4, rows.size());
    for (List<Object> row : rows) {
      String pattern = (String) row.get(0);
      assertTrue("unexpected pattern: " + pattern, expectedSamples.containsKey(pattern));
      assertEquals("count for " + pattern, 2L, asLong(row.get(1)));
      assertListEqualsIgnoreOrder(expectedSamples.get(pattern), asStringList(row.get(2)));
    }
  }

  @Test
  public void testBrainAggregationMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=brain mode=aggregation"
                    + " show_numbered_token=true variable_count_threshold=5",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    // Count, token layout and sample content are shard-invariant; only the ordering within the
    // token value lists and sample_logs varies. Assert both order-insensitively.
    Map<String, List<String>> expectedSamples = new HashMap<>();
    expectedSamples.put(PAT_NUMBERED_VERIF, Arrays.asList(LOG_VERIF_4, LOG_VERIF_6));
    expectedSamples.put(PAT_NUMBERED_ADDBLK, Arrays.asList(LOG_ADDBLK_1, LOG_ADDBLK_3));
    expectedSamples.put(PAT_NUMBERED_ALLOC, Arrays.asList(LOG_ALLOC_2, LOG_ALLOC_5));
    expectedSamples.put(PAT_NUMBERED_PACKET, Arrays.asList(LOG_PACKET_7, LOG_PACKET_8));

    Map<String, Map<String, List<String>>> expectedTokens = new HashMap<>();
    expectedTokens.put(
        PAT_NUMBERED_VERIF,
        Map.of(
            "<token1>", Arrays.asList("for", "for"),
            "<token2>", Arrays.asList("-1547954353065580372", "6996194389878584395")));
    expectedTokens.put(
        PAT_NUMBERED_ADDBLK,
        Map.of(
            "<token1>", Arrays.asList("10.251.31.85:50010", "10.251.107.19:50010"),
            "<token2>", Arrays.asList("-7017553867379051457", "-3249711809227781266"),
            "<token3>", Arrays.asList("67108864", "67108864")));
    expectedTokens.put(
        PAT_NUMBERED_ALLOC,
        Map.of(
            "<token1>", Arrays.asList("BLOCK*", "BLOCK*"),
            "<token2>", Arrays.asList("200811092030", "200811092030"),
            "<token3>", Arrays.asList("0002", "0002"),
            "<token4>", Arrays.asList("000296", "000318"),
            "<token5>", Arrays.asList("0", "0"),
            "<token6>", Arrays.asList("-00296.", "-00318."),
            "<token7>", Arrays.asList("-6620182933895093708", "2096692261399680562")));
    expectedTokens.put(
        PAT_NUMBERED_PACKET,
        Map.of(
            "<token1>", Arrays.asList("for", "for"),
            "<token2>", Arrays.asList("6996194389878584395", "-1547954353065580372")));

    List<List<Object>> rows = dataRows(result);
    assertEquals(4, rows.size());
    for (List<Object> row : rows) {
      String pattern = (String) row.get(0);
      assertTrue("unexpected pattern: " + pattern, expectedSamples.containsKey(pattern));
      assertEquals("count for " + pattern, 2L, asLong(row.get(1)));
      assertTokensEqualIgnoreOrder(expectedTokens.get(pattern), row.get(2));
      assertListEqualsIgnoreOrder(expectedSamples.get(pattern), asStringList(row.get(3)));
    }
  }

  @Test
  public void testBrainAggregationModeWithGroupByClause_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content by level method=BRAIN"
                    + " mode=aggregation show_numbered_token=true max_sample_count=5"
                    + " variable_count_threshold=2 frequency_threshold_percentage=0.2",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("level", "string"),
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));

    // variable_count_threshold=2 keeps more literals fixed, so the patterns differ from the
    // ungrouped tests. level, pattern, count and content are shard-invariant; only the ordering
    // inside token lists and sample_logs varies.
    final String vPat = "Verification succeeded for blk_<token1>";
    final String aPat =
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2>"
            + " size <token3>";
    final String allocPat =
        "BLOCK* NameSystem.allocateBlock:"
            + " /user/root/sortrand/_temporary/_task_<token1>_<token2>_r_<token3>_<token4>/part<token5>"
            + " blk_<token6>";
    final String pPat = "PacketResponder failed for blk_<token1>";

    Map<String, String> expectedLevel = new HashMap<>();
    expectedLevel.put(vPat, "INFO");
    expectedLevel.put(aPat, "INFO");
    expectedLevel.put(allocPat, "INFO");
    expectedLevel.put(pPat, "WARN");

    Map<String, List<String>> expectedSamples = new HashMap<>();
    expectedSamples.put(vPat, Arrays.asList(LOG_VERIF_4, LOG_VERIF_6));
    expectedSamples.put(aPat, Arrays.asList(LOG_ADDBLK_1, LOG_ADDBLK_3));
    expectedSamples.put(allocPat, Arrays.asList(LOG_ALLOC_2, LOG_ALLOC_5));
    expectedSamples.put(pPat, Arrays.asList(LOG_PACKET_7, LOG_PACKET_8));

    Map<String, Map<String, List<String>>> expectedTokens = new HashMap<>();
    expectedTokens.put(
        vPat, Map.of("<token1>", Arrays.asList("-1547954353065580372", "6996194389878584395")));
    expectedTokens.put(
        aPat,
        Map.of(
            "<token1>", Arrays.asList("10.251.31.85:50010", "10.251.107.19:50010"),
            "<token2>", Arrays.asList("-7017553867379051457", "-3249711809227781266"),
            "<token3>", Arrays.asList("67108864", "67108864")));
    expectedTokens.put(
        allocPat,
        Map.of(
            "<token1>", Arrays.asList("200811092030", "200811092030"),
            "<token2>", Arrays.asList("0002", "0002"),
            "<token3>", Arrays.asList("000296", "000318"),
            "<token4>", Arrays.asList("0", "0"),
            "<token5>", Arrays.asList("-00296.", "-00318."),
            "<token6>", Arrays.asList("-6620182933895093708", "2096692261399680562")));
    expectedTokens.put(
        pPat, Map.of("<token1>", Arrays.asList("6996194389878584395", "-1547954353065580372")));

    List<List<Object>> rows = dataRows(result);
    assertEquals(4, rows.size());
    for (List<Object> row : rows) {
      String pattern = (String) row.get(1);
      assertTrue("unexpected pattern: " + pattern, expectedLevel.containsKey(pattern));
      assertEquals("level for " + pattern, expectedLevel.get(pattern), row.get(0));
      assertEquals("count for " + pattern, 2L, asLong(row.get(2)));
      assertTokensEqualIgnoreOrder(expectedTokens.get(pattern), row.get(3));
      assertListEqualsIgnoreOrder(expectedSamples.get(pattern), asStringList(row.get(4)));
    }
  }

  @Test
  public void testBrainParseWithUUID_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d"
                    + " user_currency=USD' | head 1 | patterns body method=BRAIN mode=label |"
                    + " fields patterns_field",
                Index.WEBLOG.getName()));
    verifySchema(result, schema("patterns_field", "string"));
    verifyDataRows(result, rows("[PlaceOrder] user_id=<*UUID*> user_currency=USD"));
  }

  @Test
  public void testBrainParseWithUUID_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d"
                    + " user_currency=USD' | head 1 | patterns body method=BRAIN mode=label"
                    + " show_numbered_token=true | fields patterns_field, tokens",
                Index.WEBLOG.getName()));
    verifySchema(result, schema("patterns_field", "string"), schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "[PlaceOrder] user_id=<token1> user_currency=USD",
            ImmutableMap.of("<token1>", ImmutableList.of("d664d7be-77d8-11f0-8880-0242f00b101d"))));
  }

  // ---- Multi-shard patterns helpers ---------------------------------------------------------

  // hdfs_logs content lines (suffix = _id) and their BRAIN aggregation-mode pattern labels.
  private static final String LOG_ADDBLK_1 =
      "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
          + " blk_-7017553867379051457 size 67108864";
  private static final String LOG_ADDBLK_3 =
      "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
          + " blk_-3249711809227781266 size 67108864";
  private static final String LOG_ALLOC_2 =
      "BLOCK* NameSystem.allocateBlock:"
          + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
          + " blk_-6620182933895093708";
  private static final String LOG_ALLOC_5 =
      "BLOCK* NameSystem.allocateBlock:"
          + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
          + " blk_2096692261399680562";
  private static final String LOG_VERIF_4 = "Verification succeeded for blk_-1547954353065580372";
  private static final String LOG_VERIF_6 = "Verification succeeded for blk_6996194389878584395";
  private static final String LOG_PACKET_7 = "PacketResponder failed for blk_6996194389878584395";
  private static final String LOG_PACKET_8 = "PacketResponder failed for blk_-1547954353065580372";

  private static final String PAT_ADDBLK =
      "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>";
  private static final String PAT_ALLOC =
      "<*> NameSystem.allocateBlock:"
          + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>";
  private static final String PAT_VERIF = "Verification succeeded <*> blk_<*>";
  private static final String PAT_PACKET = "PacketResponder failed <*> blk_<*>";

  private static final String PAT_NUMBERED_ADDBLK =
      "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2> size"
          + " <token3>";
  private static final String PAT_NUMBERED_ALLOC =
      "<token1> NameSystem.allocateBlock:"
          + " /user/root/sortrand/_temporary/_task_<token2>_<token3>_r_<token4>_<token5>/part<token6>"
          + " blk_<token7>";
  private static final String PAT_NUMBERED_VERIF = "Verification succeeded <token1> blk_<token2>";
  private static final String PAT_NUMBERED_PACKET = "PacketResponder failed <token1> blk_<token2>";

  private static final Pattern EMAIL = Pattern.compile("^[^@]+@[^.]+\\.[^.]+$");

  /** Materialize {@code datarows} into a list of rows, mapping JSON null to Java {@code null}. */
  private static List<List<Object>> dataRows(JSONObject response) {
    List<List<Object>> rows = new ArrayList<>();
    JSONArray arr = response.getJSONArray("datarows");
    for (int i = 0; i < arr.length(); i++) {
      JSONArray r = arr.getJSONArray(i);
      List<Object> row = new ArrayList<>();
      for (int j = 0; j < r.length(); j++) {
        row.add(r.isNull(j) ? null : r.get(j));
      }
      rows.add(row);
    }
    return rows;
  }

  private static long asLong(Object value) {
    return ((Number) value).longValue();
  }

  private static List<String> asStringList(Object cell) {
    List<String> out = new ArrayList<>();
    JSONArray arr = (JSONArray) cell;
    for (int i = 0; i < arr.length(); i++) {
      out.add(arr.isNull(i) ? null : String.valueOf(arr.get(i)));
    }
    return out;
  }

  /** A patterns {@code tokens} struct as a map of token name to its captured value list. */
  private static Map<String, List<String>> tokenMap(Object cell) {
    Map<String, List<String>> out = new HashMap<>();
    JSONObject obj = (JSONObject) cell;
    for (String key : obj.keySet()) {
      out.put(key, asStringList(obj.get(key)));
    }
    return out;
  }

  private static List<String> sortedStr(List<String> in) {
    List<String> copy = new ArrayList<>(in);
    copy.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
    return copy;
  }

  private static void assertListEqualsIgnoreOrder(List<String> expected, List<String> actual) {
    assertEquals(sortedStr(expected), sortedStr(actual));
  }

  private static void assertTokensEqualIgnoreOrder(
      Map<String, List<String>> expected, Object tokensCell) {
    Map<String, List<String>> actual = tokenMap(tokensCell);
    assertEquals(expected.keySet(), actual.keySet());
    for (Map.Entry<String, List<String>> e : expected.entrySet()) {
      assertEquals(
          "token " + e.getKey(), sortedStr(e.getValue()), sortedStr(actual.get(e.getKey())));
    }
  }
}
