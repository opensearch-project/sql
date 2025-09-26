# PPL Engine V3

---
## 1. Motivations

Previously, we developed [SQL engine V2](../../docs/dev/intro-v2-engine.md) to support both SQL and PPL queries. However, as the complexity of supported SQL and PPL increased, the engine's limitations became increasingly apparent. Two major issues emerged:

1. Insufficient support for complex SQL/PPL queries: The development cycle for new commands such as `Join` and `Subquery` was lengthy, and it was difficult to achieve high robustness.

2. Lack of advanced query plan optimization: The V2 engine only supports a few pushdown optimization for certain operators and lacks of mature optimization rules and cost-based optimizer like those found in traditional databases. Query performance and scalability are core to design of PPL, enabling it to efficiently handle high-performance queries and scale to support large datasets and complex queries.

### Why Apache Calcite?

Introducing Apache Calcite brings serval significant advantages:

1. Enhanced query plan optimization capabilities: Calcite's optimizer can effectively optimize execution plans for both complex SQL and PPL queries.

2. Simplified development of new commands and functions: Expanding PPL commands is one of the key targets to enhancing the PPL language. Calcite helps streamline the development cycle for new commands and functions.

3. Decoupled execution layer: Calcite can be used for both query optimization and execution, or solely for query optimization while delegating execution to other backends such as DataFusion or Velox."

Find more details in [V3 Architecture](./intro-v3-architecture.md).

---
## 2. What's New

In the initial release of the V3 engine (3.0.0), the main new features focus on enhancing the PPL language while maintaining maximum compatibility with V2 behavior.

* **[Join](../user/ppl/cmd/join.rst) Command**
* **[Lookup](../user/ppl/cmd/lookup.rst) Command**
* **[Subquery](../user/ppl/cmd/subquery.rst) Command**

V3 (Calcite integration) engine is enabled by default in 3.3.0.

---
## 3.What are Changed

### 3.1 Breaking Changes

Because of implementation changed internally, following behaviors are changed from 3.0.0. (Behaviors in V3 is correct)

|                       Item                       |    V2     |          V3          |
|:------------------------------------------------:|:---------:|:--------------------:|
|          Return type of `timestampdiff`          | timestamp |         int          |
|             Return type of `regexp`              |    int    |       boolean        |
|   Return type of `count`,`dc`,`distinct_count`   |    int    |        bigint        |
|     Return type of `ceiling`,`floor`,`sign`      |    int    | same type with input |
| like(firstname, 'Ambe_') on value "Amber JOHnny" |   true    |        false         |
| like(firstname, 'Ambe*') on value "Amber JOHnny" |   true    |        false         |
|            cast(firstname as boolean)            |   false   |         null         |
| Sum multiple `null` values when pushdown enabled |     0     |         null         |


### 3.2 Fallback Mechanism

- As v3 engine is experimental in 3.0.0, not all PPL commands could work under this new engine. Those unsupported queries will be forwarded to V2 engine by fallback mechanism. To avoid impact on your side, normally you won't see any difference in a query response. If you want to check if and why your query falls back to be handled by V2 engine, please check OpenSearch log for "Fallback to V2 query engine since ...".
- Since 3.2.0, the fallback mechanism is disabled by default, to enable fallback, set `plugins.calcite.fallback.allowed=true`.

### 3.3 Limitations

For the following commands or functions, we add some defensive restrictions to ensure security.

#### New Restrictions
- `EVAL` won't allow to use [Metadata Fields of OpenSearch](https://docs.opensearch.org/docs/latest/field-types/metadata-fields/index/) as the fields
- `RENAME` won't allow renaming to a [Metadata Fields of OpenSearch](https://docs.opensearch.org/docs/latest/field-types/metadata-fields/index/)
- `as` won't allow to use [Metadata Fields of OpenSearch](https://docs.opensearch.org/docs/latest/field-types/metadata-fields/index/) as the alias name

For the following functionalities in V3 engine, the query will be forwarded to the V2 query engine and thus you cannot use new features in [2. What's New](#2-whats-new).

#### Unsupported functionalities (up to latest)
- All SQL queries
- PPL queries against non-OpenSearch data sources
- `dedup` with `consecutive=true`
- Search relevant commands
  - AD
  - ML
  - Kmeans
- Commands with `fetch_size` parameter
- [Existed limitations of V2](intro-v2-engine.md#33-limitations)

---
## 4.How it's Implemented

If you're interested in the new query engine, please find more details in [V3 Architecture](./intro-v3-architecture.md).

---
## 5. What's Next

The following items are on our roadmap with high priority:
- Resolve the [V3 limitation](#33-limitations).
- Advancing pushdown optimization and benchmarking
- Unified the PPL syntax between [PPL-on-OpenSearch](https://github.com/opensearch-project/sql/blob/main/ppl/src/main/antlr/OpenSearchPPLParser.g4) and [PPL-on-Spark](https://github.com/opensearch-project/opensearch-spark/blob/main/ppl-spark-integration/src/main/antlr4/OpenSearchPPLParser.g4)
- Support more DSL aggregation
