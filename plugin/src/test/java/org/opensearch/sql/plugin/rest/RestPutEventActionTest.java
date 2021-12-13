package org.opensearch.sql.plugin.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RestPutEventActionTest extends TestCase {
    @Test
    public void test() throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        String json = "";
        final JsonNode jsonNode = om.readTree(json);

        final Iterator<String> iterator = jsonNode.fieldNames();
        String index = jsonNode.get("/meta/index").asText();
        String metaType = jsonNode.get("/meta/type").asText();
        String metaBucket = jsonNode.get("/meta/bucket").asText();
        String metaObject = jsonNode.get("/meta/type").asText();
        Map<String, Object> tags = new HashMap<>();
        while (iterator.hasNext()) {
            final String fieldName = iterator.next();
            if (!fieldName.equalsIgnoreCase("meta")) {
                final JsonNode node = jsonNode.get(fieldName);

                if (node.isNumber()) {
                    tags.put(fieldName, node.intValue());
                } else {
                    tags.put(fieldName, node.asText());
                }
            }
        }

        System.out.println(index);
        System.out.println(tags);
    }

}
