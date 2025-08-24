package org.opensearch.sql.calcite.remote;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();

        loadIndex(Index.BANK);

        // Create test data for string concatenation
        Request request1 = new Request("PUT", "/test_spath/_doc/1?refresh=true");
        request1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 0}\"}");
        client().performRequest(request1);

        Request request2 = new Request("PUT", "/test_spath/_doc/2?refresh=true");
        request2.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
        client().performRequest(request2);

        Request request3 = new Request("PUT", "/test_spath/_doc/3?refresh=true");
        request3.setJsonEntity("{\"doc\": \"{\\\"n\\\": 2}\"}");
        client().performRequest(request3);
    }

    @Test
    public void testEvalStringConcatenation() throws IOException {
        JSONObject result = executeQuery("source=test_eval | spath input=doc output=result path=n");
        verifySchema(
                result,
                schema("result", "string"));
        verifyDataRows(
                result,
                rows("1"),
                rows("2"),
                rows("3"));
    }
}
