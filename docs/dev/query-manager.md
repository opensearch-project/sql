## Query Manager

### New abstraction
**QueryManager is the high level interface of the core engine**, Parser parse raw query as the Plan and sumitted to QueryManager.

1. **AstBuilder** analyze raw query string and create **Statement**.
2. **QueryPlanFactory** create **Plan** for different **Statement**.
3. **QueryManager** execute **Plan**.

<img width="393" alt="Screen Shot 2022-10-25 at 5 05 46 PM" src="https://user-images.githubusercontent.com/2969395/197908941-10b0ff01-13e4-4fc9-8c28-c466abe76446.png">

Core engine define the interface of **QueryManager. Each execution engine should provide the** implementation of QueryManager which bind to execution enviroment.
**QueryManager** manage all the submitted plans and define the following interface

* submit: submit queryexecution.
* cancel: cancel query execution.
* get: get query execution info of specific query.

![image](https://user-images.githubusercontent.com/2969395/197908982-74a30a56-9b0c-4a1a-82bf-176d25e86bc1.png)

Parser parse raw query as Statement and create AbstractPlan. Each AbstractPlan decide how to execute the query in QueryManager.

![image (1)](https://user-images.githubusercontent.com/2969395/197909011-c53bf5e0-9418-4d9d-8f4d-353147158526.png)


**QueryService is the low level interface of core engine**, each **Plan** decide how to execute the query and use QueryService to analyze, plan, optimize and execute the query.

<img width="387" alt="Screen Shot 2022-10-25 at 5 07 29 PM" src="https://user-images.githubusercontent.com/2969395/197909041-9f2ecf43-24d3-4db3-8dfb-ab82afe80de9.png">


### Change of existing logic
1. Remove the schedule logic in NIO thread. After the change,
   a. Parser will be executed in NIO thread.
   b. QueryManager decide query execution strategy. e.g. OpenSearchQueryManager schedule the QueryExecution running in **sql-worker** thread pool.