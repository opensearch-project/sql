# Consolidating ECS & Otel


## Log and Event Record Definition

[Appendix A](#appendix-a-example-mappings) contains many examples that show how
existing log formats map to the fields defined below. If there are questions
about the meaning of the field reviewing the examples may be helpful.

Here is the list of fields in a log record:

Field Name     |Description
---------------|--------------------------------------------
Timestamp      |Time when the event occurred.
ObservedTimestamp|Time when the event was observed.
TraceId        |Request trace id.
SpanId         |Request span id.
TraceFlags     |W3C trace flag.
SeverityText   |The severity text (also known as log level).
SeverityNumber |Numerical value of the severity.
Body           |The body of the log record.
Resource       |Describes the source of the log.
InstrumentationScope|Describes the scope that emitted the log.
Attributes     |Additional information about the event.
