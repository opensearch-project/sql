# OpenSearch Dashboards Query Workbench

The OpenSearch Dashboards Query Workbench enables you to query your OpenSearch data using either SQL or PPL syntax from a dedicated OpenSearch Dashboards UI. You can download your query results data in JSON, JDBC, CSV and raw text formats.


## Documentation

Please see our technical [documentation](https://docs-beta.opensearch.org/) to learn more about its features.


## Setup

1. Download OpenSearch for the version that matches the [OpenSearch Dashboards version specified in package.json](./package.json#L8).
1. Download and install the most recent version of [OpenSearch SQL plugin](https://github.com/opensearch-project/sql#open-distro-for-elasticsearch-sql).
1. Download the OpenSearch Dashboards source code for the [version specified in package.json](./package.json#L8) you want to set up.

   See the [OpenSearch Dashboards contributing guide](https://github.com/opensearch-project/OpenSearch-Dashboards/blob/main/CONTRIBUTING.md) to get started.
   
1. Change your node version to the version specified in `.node-version` inside the OpenSearch Dashboards root directory.
1. cd into the OpenSearch Dashboards source code directory.
1. Check out this package from version control into the `plugins` directory.
```
git clone git@github.com:opensearch-project/sql.git plugins --no-checkout
cd plugins
echo 'workbench/*' >> .git/info/sparse-checkout
git config core.sparseCheckout true
git checkout main
```
6. Run `yarn osd bootstrap` inside `OpenSearch-Dashboards/plugins/workbench`.

Ultimately, your directory structure should look like this:

```md
.
├── OpenSearch-Dashboards
│   └── plugins
│       └── workbench
```


## Build

To build the plugin's distributable zip simply run `yarn build`.

Example output: `./build/opensearch-query-workbench-*.zip`


## Run

- `yarn start`

  Starts OpenSearch Dashboards and includes this plugin. OpenSearch Dashboards will be available on `localhost:5601`.

- `NODE_PATH=../../node_modules yarn test:jest`

  Runs the plugin tests.


## Contributing to OpenSearch SQL Workbench

- Refer to [CONTRIBUTING.md](./CONTRIBUTING.md).
- We welcome you to get involved in development, documentation, testing the OpenSearch SQL Workbench plugin. See our [CONTRIBUTING.md](./CONTRIBUTING.md) and join in.

## Bugs, Enhancements or Questions

Please file an issue to report any bugs you may find, enhancements you may need or questions you may have [here](https://github.com/opensearch-project/sql/issues).

## License

This code is licensed under the Apache 2.0 License. 

## Copyright

Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
