
# subquery

The `subquery` command allows you to embed one PPL query within another, enabling advanced filtering and data retrieval. A subquery is executed first, and its results are used by the outer query for filtering, comparison, or joining.

Common use cases for subqueries include:

* Filtering data based on the results of another query.
* Checking for the existence of related data.
* Performing calculations that rely on aggregated values from other tables.
* Creating complex joins with dynamic conditions.

## Syntax

The `subquery` command has the following syntax:

`subquery: [ source=... | ... | ... ]`  

Subqueries use the same syntax as regular PPL queries but must be enclosed in square brackets. There are four main subquery types:

- [`IN`](#in-subquery)
- [`EXISTS`](#exists-subquery)
- [Scalar](#scalar-subquery)
- [Relation](#relation-subquery)

### IN subquery

Tests whether a field value exists in the results of a subquery:
  
```ppl ignore
where <field> [not] in [ source=... | ... | ... ]
```

The following are examples of the `IN` subquery syntax:

```ppl ignore
source = outer | where a in [ source = inner | fields b ]
source = outer | where (a) in [ source = inner | fields b ]
source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]
source = outer | where a not in [ source = inner | fields b ]
source = outer | where (a) not in [ source = inner | fields b ]
source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]
source = outer a in [ source = inner | fields b ] // search filtering with subquery
source = outer a not in [ source = inner | fields b ] // search filtering with subquery
source = outer | where a in [ source = inner1 | where b not in [ source = inner2 | fields c ] | fields b ] // nested
source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c //as join filter
```
  
### EXISTS subquery

Tests whether a subquery returns any results:
  
```ppl ignore
where [not] exists [ source=... | ... | ... ]
```

The following are examples of the `EXISTS` subquery syntax:

```ppl ignore
// Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table nested
source = outer | where exists [ source = inner | where a = c ]
source = outer | where not exists [ source = inner | where a = c ]
source = outer | where exists [ source = inner | where a = c and b = d ]
source = outer | where not exists [ source = inner | where a = c and b = d ]
source = outer exists [ source = inner | where a = c ] // search filtering with subquery
source = outer not exists [ source = inner | where a = c ] // search filtering with subquery
source = table as t1 exists [ source = table as t2 | where t1.a = t2.a ] //table alias is useful in exists subquery
source = outer | where exists [ source = inner1 | where a = c and exists [ source = nested | where c = e ] ] //nested
source = outer | where exists [ source = inner1 | where a = c | where exists [ source = nested | where c = e ] ] //nested
source = outer | where exists [ source = inner | where c > 10 ] //uncorrelated exists
source = outer | where not exists [ source = inner | where c > 10 ] //uncorrelated exists
source = outer | where exists [ source = inner ] | eval l = "nonEmpty" | fields l //special uncorrelated exists
```
  
### Scalar subquery

Returns a single value that can be used in comparisons or calculations:   
  
```ppl ignore
where <field> = [ source=... | ... | ... ]
```

The following are examples of the scalar subquery syntax:

```ppl ignore
//Uncorrelated scalar subquery in Select
source = outer | eval m = [ source = inner | stats max(c) ] | fields m, a
source = outer | eval m = [ source = inner | stats max(c) ] + b | fields m, a
//Uncorrelated scalar subquery in Where**
source = outer | where a > [ source = inner | stats min(c) ] | fields a
//Uncorrelated scalar subquery in Search filter
source = outer a > [ source = inner | stats min(c) ] | fields a
//Correlated scalar subquery in Select
source = outer | eval m = [ source = inner | where outer.b = inner.d | stats max(c) ] | fields m, a
source = outer | eval m = [ source = inner | where b = d | stats max(c) ] | fields m, a
source = outer | eval m = [ source = inner | where outer.b > inner.d | stats max(c) ] | fields m, a
//Correlated scalar subquery in Where
source = outer | where a = [ source = inner | where outer.b = inner.d | stats max(c) ]
source = outer | where a = [ source = inner | where b = d | stats max(c) ]
source = outer | where [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a
//Correlated scalar subquery in Search filter
source = outer a = [ source = inner | where b = d | stats max(c) ]
source = outer [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a
//Nested scalar subquery
source = outer | where a = [ source = inner | stats max(c) | sort c ] OR b = [ source = inner | where c = 1 | stats min(d) | sort d ]
source = outer | where a = [ source = inner | where c =  [ source = nested | stats max(e) by f | sort f ] | stats max(d) by c | sort c | head 1 ]
```
  
### Relation subquery

Used in `join` operations to provide dynamic right-side data:  
  
```ppl ignore
| join ON condition [ source=... | ... | ... ]
```

The following are examples of the relation subquery syntax:

```ppl ignore
source = table1 | join left = l right = r on condition [ source = table2 | where d > 10 | head 5 ] //subquery in join right side
source = [ source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ] | stats count(a) by b ] as outer | head 1
```

## Configuration

The `subquery` command behavior is configured using the `plugins.ppl.subsearch.maxout` setting, which specifies the maximum number of rows to return from the subsearch. Default is `10000`. A value of `0` indicates that the restriction is unlimited.

To update the setting, send the following request:

```bash ignore
PUT /_plugins/_query/settings
{
  "persistent": {
    "plugins.ppl.subsearch.maxout": "0"
  }
}
```
  

## Example 1: TPC-H q20

The following query demonstrates a complex TPC-H query 20 implementation using nested subqueries:

```ppl ignore  
source = supplier
| join ON s_nationkey = n_nationkey nation
| where n_name = 'CANADA'
  and s_suppkey in [
    source = partsupp
    | where ps_partkey in [
        source = part
        | where like(p_name, 'forest%')
        | fields p_partkey
      ]
      and ps_availqty > [
        source = lineitem
        | where l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date('1994-01-01')
          and l_shipdate < date_add(date('1994-01-01'), interval 1 year)
        | stats sum(l_quantity) as sum_l_quantity
        | eval half_sum_l_quantity = 0.5 * sum_l_quantity // Stats and Eval commands can combine when issues/819 resolved
        | fields half_sum_l_quantity
      ]
    | fields ps_suppkey
  ]
```
  

## Example 2: TPC-H q22

The following query demonstrates a TPC-H query 22 implementation using `EXISTS` and scalar subqueries:

```ppl ignore
source = [
  source = customer
    | where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > [
          source = customer
          | where c_acctbal > 0.00
            and substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
          | stats avg(c_acctbal)
        ]
      and not exists [
          source = orders
          | where o_custkey = c_custkey
        ]
    | eval cntrycode = substring(c_phone, 1, 2)
    | fields cntrycode, c_acctbal
  ] as custsale
| stats count() as numcust, sum(c_acctbal) as totacctbal by cntrycode
| sort cntrycode
```
