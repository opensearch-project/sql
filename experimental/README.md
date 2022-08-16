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

## Observability Schema 

In many regards, observability and security event share many common aspects and features that are an important concern for similar or event the same stakeholders.

Many attempts at a common data format for security/observability - type events have been created over the years:
 - CEF (Common Event Format) from ArcSight
 - LEEF (Log Event Extended Format) from IBM,
 - XML-based Common Information Model (CIM)
 
None of these formats has become the true industry standard, and while many observability tools and appliances support export into one of these data formats, it is just as common to see data being emitted by logging tools using Syslog or CSV formats.
At the same time, the rise of SaaS tools and APIs means that more and more data is being shared in JSON format, which often doesn’t translate well to older, less-extensible formats.


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

## Schema Structure & Domain Entities

As states in the introduction, the 3 pillars of the observability are the Logs, Traces & Metrics

### Logs
The log's schema mostly follows ECS (Elastic Common Schema) methodology regarding the entities and their fields.

The base entity holds the common fields for the top level events - these include:
 - timestamp - Date/time when the event originated.
 - labels - Custom key/value pairs, can be used to add meta information to events
 - tags - List of keywords used to tag each event.

All the deriving events share these fields and add additional related content.

#### Log static classification
At a high level, the log classification technique provides fields to classify events in two different ways:
 - "Where it’s from" (e.g., event.module, event.dataset, agent.type, observer.type, etc.)
 - "What it is." The categorization fields hold the "What it is" information, independent of the source of the events.

The entity that construct the classifications:
 
```graphql
type Categorization {
    #'This value indicates an event such as an alert or notable event,
    #          triggered by a detection rule executing externally to the Elastic Stack.
    #
    #          `event.kind:alert` is often populated for events coming from firewalls,
    #          intrusion detection systems, endpoint detection and response systems, and
    #          so on.
    #
    #          This value is not used by Elastic solutions for alert documents that are
    #          created by rules executing within the Kibana alerting framework.'
    kind: EventKind!
    #'This is one of four ECS Categorization Fields, and indicates the
    #        second level in the ECS category hierarchy.
    #
    #        `event.category` represents the "big buckets" of ECS categories. For example,
    #        filtering on `event.category:process` yields all events relating to process
    #        activity. This field is closely related to `event.subCategory`
    #
    #        This field is an array. This will allow proper categorization of some events
    #        that fall in multiple categories.'
    category: Categories
    #  'This is one of four ECS Categorization Fields, and indicates the
    #        third level in the ECS category hierarchy.
    #
    #        `event.subCategory` represents a categorization "sub-bucket" that, when used along
    #        with the `event.category` field values, enables filtering events down to a
    #        level appropriate for single visualization.
    #
    #        This field is an array. This will allow proper categorization of some events
    #        that fall in multiple event types.'
    subCategory: SubCategories
    #    'This is one of four ECS Categorization Fields, and indicates the
    #        lowest level in the ECS category hierarchy.
    #
    #        `event.outcome` simply denotes whether the event represents a success or a
    #        failure from the perspective of the entity that produced the event.
    #
    #        Note that when a single transaction is described in multiple events, each
    #        event may populate different values of `event.outcome`, according to their
    #        perspective.
    #
    #        Also note that in the case of a compound event (a single event that contains
    #        multiple logical events), this field should be populated with the value that
    #        best captures the overall success or failure from the perspective of the event
    #        producer.
    #
    #        Further note that not all events will have an associated outcome. For example,
    #        this field is generally not populated for metric events, events with `event.type:info`,
    #        or any events for which an outcome does not make logical sense.'
    outcome: EventOutcome
}
```

As shown in the concrete schema, the Categorization is composed of 4 fields which form togather both the 'origin' and the 'purpose' of the event.

Each event entity contains this classification element.

#### Log dynamic classification
Data stream naming scheme uses the value of the data stream fields combine to the name of the actual data stream in the following manner: {dataStream.type}-{dataStream.dataset}-{dataStream.namespace}. 

This means the fields can only contain characters that are valid as part of names of data streams

```graphql
# data stream naming scheme uses the value of the data stream fields combine to the name of the actual data stream in the following manner: {data_stream.type}-{data_stream.dataset}-{data_stream.namespace}. This means the fields can only contain characters that are valid as part of names of data streams
type StreamSet {
    #An overarching type for the data stream - 
    streamType: StreamType
    #    A user defined namespace. Namespaces are useful to allow grouping of data.
    #
    # Many users already organize their indices this way, and the data stream naming scheme now provides this best practice as a default. Many users will populate this field with default.
    #    If no value is used, it falls back to default.
    namespace:String
    #    The field can contain anything that makes sense to signify the source of the data.
    # Examples include nginx.access, prometheus, endpoint etc. For data streams that otherwise fit, but that do not have dataset set we use the value "generic" for the dataset value.
    #    event.dataset should have the same value as data_astream.dataset.
    dataset:String
}
```

This additional dynamic customizable classification field simplify the distinctions of logs arriving for specific customer-meaningfull sources. 
The classification of the streams is divided into 3 categories:
 - stream type - the distinction of the logs   
   - logs
   - metrics
   - traces
   - synthetics

 - stream name - identifies the name of the stream - for example its purpose, service component, region 
 - stream custom name - identifies some customer distinction - for example environment type (dev,test,prod)

As the log data is split up per data set across multiple data streams, each data stream contains a minimal set of fields. This leads to better space efficiency and faster queries.
More granular control of the data, having the data split up by data set and namespace allows granular control over retention and security .
Flexibility, users can use the namespace to divide and organize data in any way they want.

#### Logs Record Structure

Every signal arriving from an observing entity (_An observer is defined as a special network, security, or application device used to detect, observe, or create network, security, or application-related events and metrics_)
has the following basic composition:

```graphql
# top most level structuring an incoming format of any type of log
type LogRecord @model {
    #    The event's common characteristics
    event: Event! @relation(mappingType: "embedded")
    #    A list of top-level observations which describe 'things' that happened, where observed and reported
    observations: [BaseRecord] @relation(mappingType: "nested")
}
```

This general purpose log container reflects the possible different observations that are reported. The Event entity represents metadata related
concerns of the log itself such as:
 - classification - as described above
 - module -  Name of the module this data is coming from.
 - action -  The action captured by the event
 - time   -  Additional time aspect of the event (created,ingested,start,end)
 - stream -  as described above

**Examples**

TODO - add examples
```json
 {
    
  }
```

### Traces

As described in the introduction, a Span represents a unit of work or operation. It tracks specific operations that a request makes, describing what happened during the time in which that operation was executed.
A Span contains name, time-related data, structured log messages, and other metadata (i.e. Attributes) to provide information about the operation it tracks.

#### Distributed Traces

A Distributed Trace, more commonly known as a Trace, records the paths taken by requests (made by an application or end-user) as they propagate through the different layers of the architectures.
Trace improves the visibility of an application or system’s health and allows to understand debug behavior that is difficult to reproduce locally.
A Trace is made of one or more Spans - yhe first Span represents the Root Span. Each Root Span represents a request from start to finish and the Spans underneath the parent provide a more in-depth context of what occurs during a request (or what steps make up a request).

#### Span Categorization:
**SpanKind** describes the relationship between the Span, its parents, and its children in a Trace. SpanKind describes two independent properties that benefit tracing systems during analysis.

```graphql
enum SpanKind{
    #Indicates that the span covers server-side handling of a synchronous RPC or other remote request. This span is often the child of a remote CLIENT span that was expected to wait for a response.
    SERVER
    #     Indicates that the span describes a request to some remote service. This span is usually the parent of a remote SERVER span and does not end until the response is received.
    CLIENT
    #    Indicates that the span describes the initiators of an asynchronous request. This parent span will often end before the corresponding child CONSUMER span, possibly even before the child span starts. In messaging scenarios with batching, tracing individual messages requires a new PRODUCER span per message to be created.
    PRODUCER
    #     Indicates that the span describes a child of an asynchronous PRODUCER request.
    CONSUMER
    #     Default value. Indicates that the span represents an internal operation within an application, as opposed to an operations with remote parents or children.
    INTERNAL
}
```
Span's Context represents all the information that identifies Span in the Trace and MUST be propagated to child Spans and across process boundaries.

```graphql
# Represents all the information that identifies Span in the Trace and MUST be propagated to child Spans and across process boundaries.
# A SpanContext contains the tracing identifiers and the options that are propagated from parent to child Spans.
# In ECS AKA - https://github.com/elastic/ecs/blob/main/schemas/tracing.yml
type SpanContext {
    # A unique identifier for a trace. All spans from the same trace share
    # the same `trace_id`.
    traceId: String!
    # A unique identifier for a span within a trace, assigned when the span
    # is created.
    spanId:ID!
    # Carries tracing-system specific context in a list of key value pairs.
    # Tracestate allows different vendors propagate additional information and inter-operate with their legacy Id formats
    tracestate:JSON

}
```

A span represents a single operation within a trace. Spans can be nested to form a trace tree. Spans may also be linked to other spans from the same or different trace.
And form graphs. Often, a trace contains a root span that describes the end-to-end latency, and one or more subspans for its sub-operations. 
A trace can also contain multiple root spans, or none at all. Spans do not need to be contiguous - there may be gaps or overlaps between spans in a trace.
```graphql
type Span @model{
    id:SpanContext!
    parentId:SpanContext
    name: String
    # timestamp of the span
    start: Time
    end: Time
    #
    events:[Event]
    spanKind:SpanKind
    # A Span may be linked to zero or more other Spans (defined by SpanContext) that are causally related.
    links:[SpanContext]
    # Span status is mapped to outcome
    outcome:EventOutcome
    # Key-Value pairs representing vendor specific properties
    attributes:JSON

}
```

### Metrics
Metrics are everywhere, they can be generated from logs information within logs or summarization of numerical values and counts.
As described in the introduction section, a metrics comprises a set of dimensions, and a list of timestamp and a value tuples.

A metrics can originate from an agent sampling some features on the observed machine or by actually performing a statistical action on top of the raw logs.

The possible types of metrics are
 - **Gauge** - Gauge represents the type of a scalar metric that always exports the "current value" for every data point.
 - **Sum**   - Sum represents the type of a scalar metric that is calculated as a sum of all reported measurements over a time interval.
 - **Histogram**  - Histogram represents the type of a metric that is calculated by aggregating as a Histogram of all reported measurements over a time interval.
 - **ExponentialHistogram** -ExponentialHistogram represents the type of a metric that is calculated by aggregating as a ExponentialHistogram of all reported double measurements over a time interval.
 - **Summary**  - Summary metric data are used to convey quantile summaries data type

As stated before, every metrics has a name, type, a list of data-points:

```text
#      Metric
#   +------------+
#   |name        |
#   |description |
#   |unit        |     +------------------------------------+
#   |data        |---> |Gauge, Sum, Histogram, Summary, ... |
#   +------------+     +------------------------------------+

```

The _**data**_ container is the set of datapoints belonging to the specific metrics, we can actually think of the data-points as a time series of samples for a specific feature with timestamp and set of labels.

```text
#     Data [One of Gauge, Sum, Histogram, Summary, ...]
#   +-----------+
#   |...        |  // Metadata about the Data.
#   |points     |--+
#   +-----------+  |
#                  |      +---------------------------+
#                  |      |DataPoint 1                |
#                  v      |+------+------+   +------+ |
#               +-----+   ||label |label |...|label | |
#               |  1  |-->||value1|value2|...|valueN| |
#               +-----+   |+------+------+   +------+ |
#               |  .  |   |+-----+                    |
#               |  .  |   ||value|                    |
#               |  .  |   |+-----+                    |
#               |  .  |   +---------------------------+
#               |  .  |                   .
#               |  .  |                   .
#               |  .  |                   .
#               |  .  |   +---------------------------+
#               |  .  |   |DataPoint M                |
#               +-----+   |+------+------+   +------+ |
#               |  M  |-->||label |label |...|label | |
#               +-----+   ||value1|value2|...|valueN| |
#                         |+------+------+   +------+ |
#                         |+-----+                    |
#                         ||value|                    |
#                         |+-----+                    |
#                         +---------------------------+
# 

```
This is the graphQL schematic representations:

```graphql
type Metrics @model{
    name:String
    description:String
    # unit in which the metric value is reported. Follows the format
    # described by http://unitsofmeasure.org/ucum.html.
    unit:String
    # Data determines the aggregation type (if any) of the metric, what is the
    #   reported value type for the data points, as well as the relatationship to the time interval over which they are reported.
    data:MetricsData
}

type MetricsData {
    points:[DataPoint]
    aType:AggType
}

interface DataPoint {
   #    Attributes includes key-value pairs associated with the data point (AKA dimensions)
   attributes:JSON
   #    time field is set to the end time of the aggregation
   time:Time
   # StartTimeUnixNano in general allows detecting when a sequence of observations is unbroken.  This field indicates to consumers the
   # start time for points with cumulative and delta AggregationTemporality, and it should be included whenever possible
   # to support correct rate calculation.
   startTime:Time
}

# NumberDataPoint is a single data point in a timeseries that describes the time-varying scalar value of a metric.
type NumberDataPoint implements DataPoint {
   value:Float
}

#  Gauge represents the type of a scalar metric that always exports the
#  "current value" for every data point. It should be used for an "unknown"
#  aggregation.
type Gauge implements MetricsData{
   points:[NumberDataPoint]
   aType:AggType
}
```

--------------

## OpenSearch Observability Index Generation Support
In order for the observability analytics dashboards to take the full power of this schema, we need to support it using structured indices.
Utilizing the code generation capabilities of graphQL based schema we will create a template generator which is based on these definitions.

Each type of generator will be activated using a CLI. 

The template generator engine will work in the following steps:

```text
# 
# 1 parse & validate observability graphQL SDL ->  
# 2                                            create internal ontological layer -> 
# 3                                                                              generate composable index mapping template
# 4                                                                              assemble composite index template for specific needs 
#  
```
1) First Step will create two intermediate file representation of each graphQL schema elements:
   - {entity_name}.json file representing the element's ontology ( enumerations, fields, relationships ...) 
   - {entity_name}-index.json file representing the element's physical index configuration ( Index type, inner objects nesting, index sorting ...)

2) Second Step will generate a set of index templates which are composable template mappings that can be used together in a composite template.
For additional information check Appendix A,B 

### Index Template Mapping Composition
In Order to fully utilize the composable nature of the Observability building blocks, we are using the composable index template mapping capability.

Observability schema comes with a defined set of entities, these entities can be used as building block with the idex-template-mapping generator to
create in advanced a structured index containing specific type of entities (logs) that can be used for many purposes:
 - Observability dashboard
 - Statistics and aggregations
 - Build for reports

# Example
Let's use the next log type entities to compose a specific index for a particular aggregation purpose:

 TODO - 


## Conclusion
// TODO

--------------


## Appendix A:  Ontology Definition Language

GraphQL describes very well the structure and composition of entities, including the queries and API. GraphQL lacks the description of
explicit defining relationships between entities. It does so by implicitly understanding the composition tree structure of the entities and in
the specific way each parser translates the **_@relation_** directives.

Using a dedicated language to describe the ontology () we can further enrich our understanding of the schema and explicitly define these relationships
as a fully qualified member of the schema language.

Once translating the graphQL into an internal representation of the domain - give us an addition phase to add custom related transformations
and the ability to decouple the exact usage pattern in the underlying storage engine from the logical concepts.

Let's review the a 'Client' event entity - once in the GraphQL schema format and next in the ontological description.

```graphql
#A client is defined as the initiator of a network connection for events
#    regarding sessions, connections, or bidirectional flow records.
#
#    For TCP events, the client is the initiator of the TCP connection that sends the
#    SYN packet(s). For other protocols, the client is generally the initiator or requestor
#    in the network transaction. Some systems use the term "originator" to refer the
#    client in TCP connections. The client fields describe details about the system
#    acting as the client in the network event. Client fields are usually populated
#    in conjunction with server fields. Client fields are generally not populated for
#    packet-level events.
#
#    Client / server representations can add semantic context to an exchange, which
#    is helpful to visualize the data in certain situations. If your context falls
#    in that category, you should still ensure that source and destination are filled
#    appropriately.
type Client implements BaseRecord @model{
    # ... Skipping the implemented base record fields for readability sake...
    
    #    Client network address
    address: String
    #
    as:AutonomousSystem @relation(mappingType: "embedded")
    #    The domain name of the client system.
    domain:String
    #    Bytes sent from the client to the server
    bytes:Long
    #    geographic related fields deriving from client's location
    geo:Geo
    #    Translated IP of source based NAT sessions (e.g. internal client to internet)
    natIpp:IP
    #    IP address of the client (IPv4 or IPv6).
    ip:IP
    # mac address of the client
    mac:String
    # port of the client
    port:Long
    #    Translated port of source based NAT sessions
    natPort:Long
    #    Packets sent from the client to the server
    packets:Long
    #    The highest registered client domain, stripped of the subdomain.
    registeredDomain:String
    #    The subdomain portion of a fully qualified domain name includes
    #        all of the names except the host name under the registered_domain
    subdomain:String
    #    he effective top level domain (eTLD), also known as the domain
    #        suffix, is the last part of the domain name.
    topLevelDomain:String
    #    Fields about the client side of a network connection, used with server
    user:User @relation(mappingType: "foreign")
}
```

The GraphQL schema has defined that there are 3 logical directives
- @model - which states this is a top level entity
- @relation - which states that the specific _AutonomousSystem_ & _User_ fields are nested objects with specific concerns regarding the  storage layout

The client.json generated ontology file represents these implicit concerns in a more explicit and formal manner:

```json
{
  "ont": "user",
  "directives": [],
  "entityTypes": [
    {
      "eType": "AutonomousSystem",
      "name": "AutonomousSystem",
      "directives": [
        {
          "type": "DATATYPE",
          "name": "key",
          "arguments": [
            {
              "name": "fields",
              "value": [
                "number"
              ]
            },
            {
              "name": "name",
              "value": "number"
            }
          ]
        }
      ],
      "mandatory": [
        "number"
      ],
      "properties": [
        "number",
        "organizationName"
      ],
      "abstract": false
    },
    {
      "eType": "Geo",
      "name": "Geo",
      "properties": [
        "timezone",
        "countryIsoCode",
        "regionIsoCode",
        "countryName",
        "postalCode",
        "continentCode",
        "location",
        "regionName",
        "cityName",
        "name",
        "continentName"
      ],
      "abstract": false
    },
    {
      "idField": [
        "id"
      ],
      "eType": "Group",
      "name": "Group",
      "mandatory": [
        "id"
      ],
      "properties": [
        "domain",
        "id"
      ],
      "abstract": false
    },
    {
      "idField": [
        "id"
      ],
      "eType": "User",
      "name": "User",
      "directives": [
        {
          "type": "DATATYPE",
          "name": "model"
        }
      ],
      "mandatory": [
        "id"
      ],
      "properties": [
        "fullName",
        "group",
        "domain",
        "hash",
        "id",
        "email",
        "roles",
        "name"
      ],
      "abstract": false
    }
  ],
  "relationshipTypes": [
    {
      "idField": [
        "id"
      ],
      "rType": "group",
      "name": "group",
      "directional": true,
      "ePairs": [
        {
          "name": "User->Group",
          "eTypeA": "User",
          "sideAIdField": "id",
          "eTypeB": "Group",
          "sideBIdField": "id"
        }
      ]
    }
  ],
  "properties": [{ "": "Skipping the properties for readability sake..."}],
  "enumeratedTypes": [ { "": "Skipping the enumerated types for readability sake..."}],
  "compositeTypes": []
}
```

The explicit **_relationshipTypes_** list states the first class relationship entity. An additional low level 'index-provider' instruction configuration
file is auto-generated according to this SDL file. Appendix A details the low level instructions file on how to construct the index template mappings for the above schema.

## Appendix B: Index Provider Physical Storage Configuration

The index-provider is what helps opensearch analyze the schema instruction file and assemble the required indices and mappings.

Let's review the file to understand the instructions:

```json
{
  "entities": [
    {
      "type": "User",
      "partition": "NESTED",
      "props": {
        "values": [
          "User"
        ]
      },
      "nested": [
        {
          "type": "Group",
          "partition": "NESTED",
          "props": {
            "values": [
              "Group"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        }
      ],
      "mapping": "INDEX"
    },
    {
      "type": "AutonomousSystem",
      "partition": "NESTED",
      "props": {
        "values": [
          "AutonomousSystem"
        ]
      },
      "nested": [],
      "mapping": "INDEX"
    },
    {
      "type": "Geo",
      "partition": "NESTED",
      "props": {
        "values": [
          "Geo"
        ]
      },
      "nested": [],
      "mapping": "INDEX"
    },
    {
      "type": "Group",
      "partition": "NESTED",
      "props": {
        "values": [
          "Group"
        ]
      },
      "nested": [],
      "mapping": "INDEX"
    },
    {
      "type": "Client",
      "partition": "STATIC",
      "props": {
        "values": [
          "Client"
        ]
      },
      "nested": [
        {
          "type": "User",
          "partition": "NESTED",
          "props": {
            "values": [
              "User"
            ]
          },
          "nested": [
            {
              "type": "Group",
              "partition": "NESTED",
              "props": {
                "values": [
                  "Group"
                ]
              },
              "nested": [],
              "mapping": "INDEX"
            }
          ],
          "mapping": "INDEX"
        },
        {
          "type": "AutonomousSystem",
          "partition": "NESTED",
          "props": {
            "values": [
              "AutonomousSystem"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        },
        {
          "type": "Geo",
          "partition": "NESTED",
          "props": {
            "values": [
              "Geo"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        }
      ],
      "mapping": "INDEX"
    },
    {
      "type": "User",
      "partition": "STATIC",
      "props": {
        "values": [
          "User"
        ]
      },
      "nested": [
        {
          "type": "Group",
          "partition": "NESTED",
          "props": {
            "values": [
              "Group"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        }
      ],
      "mapping": "INDEX"
    }
  ],
  "relations": [
    {
      "type": "has_User",
      "partition": "STATIC",
      "symmetric": false,
      "props": {
        "values": [
          "has_User"
        ]
      },
      "nested": [],
      "redundant": [],
      "mapping": "INDEX"
    }
  ],
  "ontology": "client",
  "rootEntities": [
    {
      "type": "Client",
      "partition": "STATIC",
      "props": {
        "values": [
          "Client"
        ]
      },
      "nested": [
        {
          "type": "User",
          "partition": "NESTED",
          "props": {
            "values": [
              "User"
            ]
          },
          "nested": [
            {
              "type": "Group",
              "partition": "NESTED",
              "props": {
                "values": [
                  "Group"
                ]
              },
              "nested": [],
              "mapping": "INDEX"
            }
          ],
          "mapping": "INDEX"
        },
        {
          "type": "AutonomousSystem",
          "partition": "NESTED",
          "props": {
            "values": [
              "AutonomousSystem"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        },
        {
          "type": "Geo",
          "partition": "NESTED",
          "props": {
            "values": [
              "Geo"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        }
      ],
      "mapping": "INDEX"
    },
    {
      "type": "User",
      "partition": "STATIC",
      "props": {
        "values": [
          "User"
        ]
      },
      "nested": [
        {
          "type": "Group",
          "partition": "NESTED",
          "props": {
            "values": [
              "Group"
            ]
          },
          "nested": [],
          "mapping": "INDEX"
        }
      ],
      "mapping": "INDEX"
    }
  ],
  "rootRelations": [
    {
      "type": "has_User",
      "partition": "STATIC",
      "symmetric": false,
      "props": {
        "values": [
          "has_User"
        ]
      },
      "nested": [],
      "redundant": [],
      "mapping": "INDEX"
    }
  ]
}
```

## Appendix C: Logs Index Implementation Considerations
// TODO

## Appendix D: Metrics Index Implementation Considerations 
// TODO