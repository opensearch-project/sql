package org.opensearch.sql.catalog.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class PrometheusCatalogMetadata extends CatalogMetadata {

  private Long defaultTimeRange;

}
