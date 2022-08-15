# Observability Schema Draft & Components
The purpose of this POC is to propose a unified schema structure to the observability domain.
This schema is largely based on former work done both by OpenTelemetry project and ElasticCommonSchema project.

Additional important aspect of this work
- generate a physical index structure and dedicated observability data views (projections) according to specific needs
- demonstrate the capability of using a High Level Schema Definition Language for description of the search domain.
- utilize the graphQL SDL tool to generate API according to the pre-defined views


## Introduction

### The 3 Pillars of Observability
Logs, metrics, and traces are known as the three pillars of observability.
While plainly having access to logs, metrics, and traces doesn’t necessarily make systems more observable, these are powerful tools that can unlock the ability to build better systems.

### Event Logs

An event log is an immutable, timestamped record of discrete events that happened over time, logs almost always carry a timestamp and a payload of some context.
Event logs are helpful when trying to uncover emergent and unpredictable behaviors exhibited by components of a distributed system.
Unfortunately simply by looking at discrete events that occurred in any given system at some point in time, it's practically impossible to determine all such triggers.

In order to actually understand the root cause of some misbehaving functionality we need to do the following:
- Infer the request lifecycle across different components of the distributed architecture

- Iteratively ask questions about interactions among various parts of the system


It is also necessary to be able to infer the fate of a system as a whole (measured over a duration that is orders of magnitudes longer than the lifecycle of a single request).

**_Traces and metrics_** are abstractions built on top of logs that pre-process and encode information along two orthogonal axes,
 - Request-centric - **_trace_**
 - System-centric - **_metric_**

#### Data Ingestion

During Ingestion, raw logs are almost always normalized, filtered, and processed by a tool like Logstash, fluentd, Scribe, or Heka before they’re persisted in a data store
Interesting observation is that logs arrive as a stream of data and can be analyzed using the streaming data analysis tool and concepts, these concepts include:
 - data enrichment
 - on flight filtering and aggregation
 - events transformation & normalization


### Metrics

Metrics are a numeric representation of data measured over intervals of time. Metrics use the power of mathematical modeling and prediction to derive knowledge of the behavior of a system over intervals of time .

Since numbers are optimized for storage, processing, compression, and retrieval, metrics enable longer retention of data as well as easier querying. Metrics are perfectly suited to building dashboards that reflect historical trends.
Metrics also allow for gradual reduction of data resolution -  after a certain period of time, data can be aggregated into daily or weekly frequency.

In a nutshell - A metric is identified using both the metric name and the labels which are also called dimensions.  The actual data stored in the time series is called a sample, and it consists of two components: a float64 value and a timestamp.
Metrics follow the 'append-only' rule and are immutable for altering the labels.

A large advantage of metrics-based monitoring over logs is that unlike log generation and storage, metrics transfer and storage has a constant overhead.
Metrics storage increases with more permutations of label values (e.g., when more hosts or containers are spun up, or when new services get added or when existing services get instrumented more),

### Traces

A drawback with both application logs and application metrics is that they are system scoped, making it hard to understand what’s happening inside a particular system.

With logs (without using some sort of joins), a single line doesn’t give much information about what happened to a request across all components of a system.

It is possible to construct a system that correlates metrics and logs across the address space or RPC boundaries using some UID. Such systems require a metric to carry a UID as a label.

Using high cardinality values like UIDs as metric labels can overwhelm time-series databases.


When used optimally, logs and metrics give us complete omniscience into a silo, but not much more.

**Distributed tracing** is a technique that addresses the problem of bringing visibility into the lifetime of a request across several systems. 

A trace is a representation of a series of causally related distributed events that encode the end-to-end request flow through a distributed system.
Traces are a representation of logs; A single trace can provide visibility into both the path traversed by a request and the structure of a request. The path of a request allow the understanding the different services involved in that path.

The basic idea behind tracing is to identify specific points (function call / RPC boundaries / threads / queues) in an application, proxy, framework, library, runtime, middleware, and anything else in the path of a request that represents the following:

- Forks in execution flow
- Exit network or process boundaries

Traces are used to identify the amount of work done at each layer while preserving causality by using happens-before semantics. A trace is a directed acyclic graph (DAG) of spans, where the edges between spans are called references.

#### Trace Parts
When a request begins, it’s assigned a globally unique ID, which is then propagated throughout the request path so that each point of instrumentation is able to insert or enrich metadata before passing the ID around to the next hop in the flow of a request.
Each hop along the flow is represented as a span, when the execution flow reaches the instrumented point at one of these services, a record is emitted along with metadata.

These records are usually logged to disk before being submitted to a collector, which then can reconstruct the flow of execution based on different records emitted by different parts of the system.

Traces are primarily for 
 - inter service dependency analysis
 - distributed profiling, and debugging
 - chargeback and capacity planning.

Zipkin and Jaeger are two of the most popular OpenTracing-compliant open source distributed tracing solutions. 

For tracing to be truly effective, every component in the path of a request needs to be modified to propagate tracing information - directly or using augmentation based libraries.

## Schema SDL

### GraphQL Schema-Definition-Language
In order to maintain a higher level of abstraction and to provide a general capability for multi-purpose usability, the popular and highly supported graphQL language is selected.
GraphQL provides a complete description of the data & gives clients the power to ask for exact & specific structured information in a simple manner. GraphQL stack offers a rich echo-system of polyglot support for code generation and endpoint libraries.

Selection to represent the Observability domain using graphQL semantics will help in all these capabilities and more 
 - Using a structured and dedicated API
 - Using standard tools for data transformation
 - Allowing Ingestion by any GraphQL compliant system

#### Example

Let's review the schema of a 'network' type of log:

```graphql
enum NetworkDirection {
    ingress
    egress
    inbound
    outbound
    internal
    external
    unknown
}

type Vlan {
    #   VLAN ID as reported by the observer.
    id:String
    #    Optional VLAN name as reported by the observer.
    name:String
}

type Network implements BaseRecord {
    timestamp : Time!
    labels : JSON
    message: String
    # List of keywords used to tag each event.
    tags: [String]
    # Key-Value pairs representing vendor specific properties
    attributes: JSON

    name:String
    application:String
    bytes:Long
    networkPackets:Long
    communityId:String
    direction:NetworkDirection
    forwardedIp:IP
    ianaNumber:String
    transport:String
    inner:JSON
    protocol:String
    aType:String
 
    vlan:Vlan @relation(mappingType: "embedded")
    innerVlan:Vlan @relation(mappingType: "embedded")
    description:String
}
```

Network is defined as a type implementing a basic interface (BaseRecord) which encapsulates common records for all events.
The schema also supports different field type such as primitives and custom types such as JSON and IP.

Network defines _NetworkDirection_ enumeration for the network event's direction which categorize the network direction.
Network also defines the vlan object structure which can be queries directly using the graphQL query language. The Vlan object
has a **_@relation_** directive assign to it under the definition of the network object.

**Directive**
Directives are an aspect based instruction to the model, they are intended to be interpreted by the parsing tool and handle the directive in
a proactive manner that will best reflect their meaning to the specific role.

In the **_@relation_** case - we can interpret this relational structure directive for multiple purposes:
 
- create a dedicated query by relation type API
- implement a specific physical optimal index in the storage layer
- understand relationships between different domain entities

Additional supported directives:
 
- @model: describing the top level log entities
- @key: describing the uniqueness and indexing field for an entity

Custom directives can be added and their interpretation is in the hands of each parser.

## Ontology Definition Language

GraphQL describes very well the structure and composition of entities, including the queries and API. GraphQL lacks the description of
explicit defining relationships between entities. It does so by implicitly understanding the composition tree structure of the entities and in
the specific way each parser translates the **_@relation_** directives.

Using a dedicated language to describe the ontology () we can further enrich our understanding of the schema and explicitly define these relationships
as a fully qualified member of the schema language.

Once translating the graphQL into an internal representation of the domain - give us an addition phase to add custom related transformations
and the ability to decouple the exact usage pattern in the underlying storage engine from the logical concepts.

Let's review the a 'Client' event entity - once in the GraphQL schema format and next in the ontological description.

```graphql

```