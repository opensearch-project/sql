package org.opensearch.sql.datasources.transport;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateUpdateDatasourceResponse{
    @JsonProperty("status")
    private String status;

    public CreateUpdateDatasourceResponse(String status) {
        this.status = status;
    }

}
