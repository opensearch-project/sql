Compatible with OpenSearch and OpenSearch Dashboards Version 2.15.0

### Features
* Support Percentile in PPL ([#2710](https://github.com/opensearch-project/sql/pull/2710))

### Enhancements
* Add option to use LakeFormation in S3Glue data source ([#2624](https://github.com/opensearch-project/sql/pull/2624))
* Remove direct ClusterState access in LocalClusterState ([#2717](https://github.com/opensearch-project/sql/pull/2717))

### Maintenance
* Use EMR serverless bundled iceberg JAR ([#2632](https://github.com/opensearch-project/sql/pull/2632))
* Update maintainers list ([#2663](https://github.com/opensearch-project/sql/pull/2663))

### Infrastructure
* Increment version to 2.15.0-SNAPSHOT ([#2650](https://github.com/opensearch-project/sql/pull/2650))

### Refactoring
* Refactor SparkQueryDispatcher ([#2636](https://github.com/opensearch-project/sql/pull/2636))
* Refactor IndexDMLHandler and related classes ([#2644](https://github.com/opensearch-project/sql/pull/2644))
* Introduce FlintIndexStateModelService ([#2658](https://github.com/opensearch-project/sql/pull/2658))
* Add comments to async query handlers ([#2657](https://github.com/opensearch-project/sql/pull/2657))
* Extract SessionStorageService and StatementStorageService ([#2665](https://github.com/opensearch-project/sql/pull/2665))
* Make models free of XContent ([#2677](https://github.com/opensearch-project/sql/pull/2677))
* Remove unneeded datasourceName parameters ([#2683](https://github.com/opensearch-project/sql/pull/2683))
* Refactor data models to be generic to data storage ([#2687](https://github.com/opensearch-project/sql/pull/2687))
* Provide a way to modify spark parameters ([#2691](https://github.com/opensearch-project/sql/pull/2691))
* Change JobExecutionResponseReader to an interface ([#2693](https://github.com/opensearch-project/sql/pull/2693))
* Abstract queryId generation ([#2695](https://github.com/opensearch-project/sql/pull/2695))
* Introduce SessionConfigSupplier to abstract settings ([#2707](https://github.com/opensearch-project/sql/pull/2707))
* Add accountId to data models ([#2709](https://github.com/opensearch-project/sql/pull/2709))
* Pass down request context to data accessors ([#2715](https://github.com/opensearch-project/sql/pull/2715))