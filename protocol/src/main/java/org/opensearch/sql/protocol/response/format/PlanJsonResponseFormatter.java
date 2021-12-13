package org.opensearch.sql.protocol.response.format;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.protocol.response.PlanQueryResult;

public class PlanJsonResponseFormatter extends JsonResponseFormatter<PlanQueryResult> {

    public PlanJsonResponseFormatter(JsonResponseFormatter.Style style) {
        super(style);
    }

    @Override
    public Object buildJsonObject(PlanQueryResult response) {
        JsonResponse.JsonResponseBuilder json = JsonResponse.builder();

        json.total(response.size())
                .size(response.size());

        json.datarows(fetchDataRows(response));
        return json.build();
    }

    private Object[] fetchDataRows(PlanQueryResult response) {
        Object[] rows = new Object[response.size()];
        int i = 0;
        for (Object values : response) {
            rows[i++] = values;
        }
        return rows;
    }

    /**
     * org.json requires these inner data classes be public (and static)
     */
    @Builder
    @Getter
    public static class JsonResponse {
        private final Object[] datarows;

        private long total;
        private long size;
    }

    @RequiredArgsConstructor
    @Getter
    public static class Column {
        private final String name;
    }

}

