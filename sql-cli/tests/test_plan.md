# Test Plan
 The purpose of this checklist is to guide you through the basic usage of OpenSearch SQL CLI, as well as a manual test process. 
 
 
## Display

* [ ] Auto-completion
    * SQL syntax auto-completion
    * index name auto-completion
* [ ] Test pagination with different output length / width. 
    * query for long results to see the pagination activated automatically.
* [ ] Test table formatted output.
* [ ] Test successful conversion from horizontal to vertical display with confirmation. 
    * resize the terminal window before launching sql cli, there will be a warning message if your terminal is too narrow for horizontal output. It will ask if you want to convert to vertical display
* [ ] Test warning message when output > 200 rows of data. (Limited by OpenSearch SQL syntax)
    * `SELECT * FROM accounts`
    * Run above command, you’ll see the max output is 200, and there will be a message at the top of your results telling you how much data was fetched.
    * If you want to query more than 200 rows of data, try add a `LIMIT` with more than 200.


## Connection

* [ ] Test connection to a local OpenSearch instance
    * [ ] OpenSearch, no authentication
    * [ ] OpenSearch, install [OpenSearch Security plugin](https://docs-beta.opensearch.org/docs/opensearch/install/plugins/) to enable authentication and SSL
    * Run command like `opensearchsql <endpoint> -u <username> -w <password>` to connect to instance with authentication.
* [ ] Test connection to [Amazon Elasticsearch domain](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-gsg.html) with
[Fine Grained Access Control](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/fgac.html) enabled. 
    * Have your aws credentials correctly configured by `aws configure`
    * `opensearchsql --aws-auth <endpoint> -u <username> -w <password>`
* [ ] Test connection fail when connecting to invalid endpoint. 
    * `opensearchsql invalidendpoint.com`


## Execution

* [ ] Test successful execution given a query. e.g.
    *  `SELECT * FROM bank WHERE age >30 AND gender = 'm'` 
* [ ] Test unsuccessful execution with an invalid SQL query will give an error message
* [ ] Test load config file 
    * `vim .config/opensearchsql-cli/config`
    * change settings such as `table_format = github`
    * restart sql cli, check the tabular output change


## Query Options

* [ ] Test explain option -e
    * `opensearchsql -q "SELECT * FROM accounts LIMIT 5;" -e`
* [ ] Test query and format option -q, -f
    * `opensearchsql -q "SELECT * FROM accounts LIMIT 5;" -f csv`
* [ ] Test vertical output option -v
    * `opensearchsql -q "SELECT * FROM accounts LIMIT 5;" -v`

## OS and Python Version compatibility

* [ ] Manually test on Linux(Ubuntu) and MacOS
* [ ] Test against python 3.X versions (optional)

