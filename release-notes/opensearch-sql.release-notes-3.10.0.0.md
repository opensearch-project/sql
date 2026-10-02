## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Add opt-in asynchronous PPL submission using `wait_for_completion_timeout` and `keep_alive`, with result retrieval through `GET /_plugins/_async_query/{id}`.

### Bug Fixes

* Release jobs that return rows, explain output, or failures inline without creating retention timers. Retain terminal results only for submissions that return a polling ID.

### Compatibility Notes

* `QueryJobService.submit` accepts a submission wait and retention duration and returns `CompletionStage<QueryResult>`. Embedding callers receive an inline result or a `RUNNING` result containing the polling ID.
* Fetching PPL async results requires `cluster:admin/opensearch/ql/async_query/result` in addition to the PPL submission permission. Updating the default security-plugin PPL role is a separate follow-up.
* PPL async cancellation and dispatching synchronous runner preparation outside the submit thread remain follow-ups.
