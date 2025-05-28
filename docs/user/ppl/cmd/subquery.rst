=============
subquery (aka subsearch)
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| (From 3.0.0)
| The subquery (aka subsearch) commands contain 4 types: ``InSubquery``, ``ExistsSubquery``, ``ScalarSubquery`` and ``RelationSubquery``. The first three are expressions, they are used in WHERE clause (``where <boolean expression>``) and search filter(``search source=* <boolean expression>``). ``RelationSubquery`` is not an expression, it is a statement.

Version
=======
3.0.0

Syntax
======
Subquery (aka subsearch) has the same syntax with search command, except that it must be enclosed in square brackets.

InSubquery::

    where <field> [not] in [ source=... | ... | ... ]

ExistsSubquery::

    where [not] exists [ source=... | ... | ... ]

ScalarSubquery::

    where <field> = [ source=... | ... | ... ]

RelationSubquery::

    | join ON condition [ source=... | ... | ... ]


Configuration
=============
This command requires Calcite enabled. In 3.0.0-beta, as an experimental the Calcite configuration is disabled by default.

Enable Calcite::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.calcite.enabled" : true
	  }
	}'

Result set::

    {
      "acknowledged": true,
      "persistent": {
        "plugins": {
          "calcite": {
            "enabled": "true"
          }
        }
      },
      "transient": {}
    }

Usage
=====

InSubquery::

    source = outer | where a in [ source = inner | fields b ]
    source = outer | where (a) in [ source = inner | fields b ]
    source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]
    source = outer | where a not in [ source = inner | fields b ]
    source = outer | where (a) not in [ source = inner | fields b ]
    source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]
    source = outer a in [ source = inner | fields b ] // search filtering with subquery
    source = outer a not in [ source = inner | fields b ] // search filtering with subquery)
    source = outer | where a in [ source = inner1 | where b not in [ source = inner2 | fields c ] | fields b ] // nested
    source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c //as join filter

ExistsSubquery::

    // Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table nested
    source = outer | where exists [ source = inner | where a = c ]
    source = outer | where not exists [ source = inner | where a = c ]
    source = outer | where exists [ source = inner | where a = c and b = d ]
    source = outer | where not exists [ source = inner | where a = c and b = d ]
    source = outer exists [ source = inner | where a = c ] // search filtering with subquery
    source = outer not exists [ source = inner | where a = c ] //search filtering with subquery
    source = table as t1 exists [ source = table as t2 | where t1.a = t2.a ] //table alias is useful in exists subquery
    source = outer | where exists [ source = inner1 | where a = c and exists [ source = nested | where c = e ] ] //nested
    source = outer | where exists [ source = inner1 | where a = c | where exists [ source = nested | where c = e ] ] //nested
    source = outer | where exists [ source = inner | where c > 10 ] //uncorrelated exists
    source = outer | where not exists [ source = inner | where c > 10 ] //uncorrelated exists
    source = outer | where exists [ source = inner ] | eval l = "nonEmpty" | fields l //special uncorrelated exists

ScalarSubquery::

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

RelationSubquery::

    source = table1 | join left = l right = r on condition [ source = table2 | where d > 10 | head 5 ] //subquery in join right side
    source = [ source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ] | stats count(a) by b ] as outer | head 1



Example 1: TPC-H q20
====================

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
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
	  """
	}'

Example 2: TPC-H q22
====================

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
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
	  """
	}'

