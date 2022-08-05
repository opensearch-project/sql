# Conforming with existing Data-Prepare/OTEL Schema

Existing mapping for data-prepare:
 - https://github.com/opensearch-project/data-prepper/tree/main/docs/schemas/trace-analytics

## Trace
    TODO
## Service
    TODO
### Data Prepare 
    TODO

### Fields
* **hashId** - A deterministic hash of this relationship.
* **kind** - The span kind, corresponding to the source of the relationship. See OpenTelemetry - SpanKind.
* **serviceName** - The name of the service which emitted the span. Currently derived from the opentelemetry.proto.resource.v1.Resource associated with the span.
* **destination.domain** - The serviceName of the service being called by this client.
* **destination.resource** - The span name (API, operation, etc.) being called by this client.
* **target.domain** - The serviceName of the service being called by a client.
* **target.resource** - The span name (API, operation, etc.) being called by a client.
* **traceGroupName** - The top-level span name which started the request chain.
---

### Opensearch Service 
    TODO

