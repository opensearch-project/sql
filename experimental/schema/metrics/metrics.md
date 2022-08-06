# Metrics Schema

Metrics are a specific kind of telemetry data. They represent a snapshot of the current state for a set of data.
They are distinct from logs or events, which focus on records or information about individual events.

This concept expresses all system states as numerical values; counts, current values, enumerations, and boolean states being common examples.
Contrary to metrics, singular events occur at a specific time. Metrics tend to aggregate data temporally. While this can lose information, the reduction in overhead is an engineering trade-off commonly chosen in many modern monitoring systems.

Time series are a record of changing information over time. While time series can support arbitrary strings or binary data, only numeric data is in our scope.
Common examples of metric time series would be network interface counters, device temperatures, BGP connection states, and alert states.

_**Data Types**_

**Values:** Metric values in MUST be either floating points or integers.

**Labels:** Labels are key-value pairs consisting of strings

**LabelSet:** A LabelSet MUST consist of list of unique Labels

**MetricPoint:** Each MetricPoint consists of a set of values, depending on the MetricFamily type.

**Exemplars:** Exemplars are references to data outside of the MetricSet (traceId for example)

**MetricFamily:** A MetricFamily MUST have a name, type, and unit metadata. Every Metric within a MetricFamily MUST have a unique LabelSet.

**Metric**  Metrics are defined by a unique LabelSet within a MetricFamily.
Metrics MUST contain a list of one or more MetricPoints. Metrics with the same name for a given MetricFamily SHOULD have the same set of label names in their LabelSet.

  * Metrics.name: String value representation of the matrix purpose
  * Metrics.type: Valid values are "unknown", "gauge", "counter", "stateset", "info", "histogram", "gaugehistogram", and "summary".
  * Metrics.Unit: specifies MetricFamily units.


## Metric Types

### Gauge

Gauges are current measurements, such as bytes of memory currently used or the number of items in a queue. For gauges the absolute value is what is of interest to a user.

**_A MetricPoint in a Metric with the type gauge MUST have a single value._**

Gauges MAY increase, decrease, or stay constant over time. Even if they only ever go in one direction, they might still be gauges and not counters.

### Counter

Counters measure discrete events. Common examples are the number of HTTP requests received, CPU seconds spent, or bytes sent. For counters how quickly they are increasing over time is what is of interest to a user.

**_A MetricPoint in a Metric with the type Counter MUST have one value called Total._** 


### StateSet

StateSets represent a series of related boolean values, also called a bitset.

A point of a StateSet metric MAY contain multiple states and MUST contain one boolean per State. States have a name which are Strings.

A StateSet Metric's LabelSet MUST NOT have a label name which is the same as the name of its MetricFamily.

This is suitable where the enum value changes over time, and the number of States isn't much more than a handful.

MetricFamilies of type StateSets MUST have an empty Unit string.

### Info

Info metrics are used to expose textual information which SHOULD NOT change during process lifetime. Common examples are an application's version, revision control commit, and the version of a compiler.

**_A MetricPoint of an Info Metric contains a LabelSet._**

MetricFamilies of type Info MUST have an empty Unit string.

### Histogram

Histograms measure distributions of discrete events. Common examples are the latency of HTTP requests, function runtimes, or I/O request sizes.

**_A Histogram MetricPoint MUST contain at least one bucket_**, and SHOULD contain Sum, and Created values. Every bucket MUST have a threshold and a value.

### GaugeHistogram

GaugeHistograms measure current distributions. Common examples are how long items have been waiting in a queue, or size of the requests in a queue.

**_A GaugeHistogram MetricPoint MUST have one bucket with an +Inf threshold_**, and SHOULD contain a Gsum value. Every bucket MUST have a threshold and a value.

The buckets for a GaugeHistogram follow all the same rules as for a Histogram.

The bucket and Gsum of a GaugeHistogram are conceptually gauges. 

### Summary

Summaries also measure distributions of discrete events and MAY be used when Histograms are too expensive and/or an average event size is sufficient.

**_A Summary MetricPoint MAY consist of a Count, Sum, Created, and a set of quantiles._**

Semantically, Count and Sum values are counters & MUST be an integer.


## References
* OpenMetrics: https://github.com/OpenObservability/OpenMetrics/blob/main/proto/openmetrics_data_model.proto
* OTEL: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto