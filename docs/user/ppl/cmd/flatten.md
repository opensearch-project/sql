# flatten  

## Description  

The `flatten` command flattens a struct or an object field into separate fields in a document.
The flattened fields will be ordered **lexicographically** by their original key names in the struct. For example, if the struct has keys `b`, `c` and `Z`, the flattened fields will be ordered as `Z`, `b`, `c`.
Note that `flatten` should not be applied to arrays. Use the `expand` command to expand an array field into multiple rows instead. However, since an array can be stored in a non-array field in OpenSearch, when flattening a field storing a nested array, only the first element of the array will be flattened.
## Syntax  

flatten \<field\> [as (\<alias-list\>)]
* field: mandatory. The field to be flattened. Only object and nested fields are supported.  
* alias-list: optional. The names to use instead of the original key names. Names are separated by commas. It is advised to put the alias-list in parentheses if there is more than one alias. The length must match the number of keys in the struct field. The provided alias names **must** follow the lexicographical order of the corresponding original keys in the struct.  
  
## Example: flatten an object field with aliases  

This example shows flattening a message object field and using aliases to rename the flattened fields.
Given the following index `my-index`
  
```text
 {"message":{"info":"a","author":"e","dayOfWeek":1},"myNum":1}
 {"message":{"info":"b","author":"f","dayOfWeek":2},"myNum":2}

```
  
with the following mapping:
  
```json
 {
   "mappings": {
     "properties": {
       "message": {
         "type": "object",
         "properties": {
           "info": {
             "type": "keyword",
             "index": "true"
           },
           "author": {
             "type": "keyword",
             "fields": {
               "keyword": {
                 "type": "keyword",
                 "ignore_above": 256
               }
             },
             "index": "true"
           },
           "dayOfWeek": {
             "type": "long"
           }
         }
       },
       "myNum": {
         "type": "long"
       }
     }
   }
 }


```
  
The following query flattens the `message` field and renames the keys to
`creator, dow, info`:
  
```ppl
source=my-index
| flatten message as (creator, dow, info)
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+-----------------------------------------+--------+---------+-----+------+
| message                                 | myNum  | creator | dow | info |
|-----------------------------------------|--------|---------|-----|------|
| {"info":"a","author":"e","dayOfWeek":1} | 1      | e       | 1   | a    |
| {"info":"b","author":"f","dayOfWeek":2} | 2      | f       | 2   | b    |
+-----------------------------------------+--------+---------+-----+------+
```
  
## Limitations  

* `flatten` command may not work as expected when its flattened fields are  
  
  invisible.
  For example in query
  `source=my-index | fields message | flatten message`, the
  `flatten message` command doesn't work since some flattened fields such as
  `message.info` and `message.author` after command `fields message` are
  invisible.
  As an alternative, you can change to `source=my-index | flatten message`.