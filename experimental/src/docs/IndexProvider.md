## Index Provider Explained
Index Provider purpose is to define a low level schematic structure configuration for the underlying physical store

The store will implement the index provider instructions according to its store architecture and capabilities


### Indexing Policy
Opensearch offers the index data structure as its main indexing facility that represents a document class 
in general this is a virtual concept which has diverse meaning over the years - see documentation
 - https://www.elastic.co/blog/index-vs-type
 - https://medium.com/@mena.meseha/understand-the-parent-child-relationship-in-elasticsearch-3c9a5a57f202)
 
### Entity-Relation-Hierarchy
We can clearly define the notion of entity that is coupled with the notion of an index, this notion can be expanded using
the different tools available on OpenGraph such as: 

- Redundancy  - Type that contains redundant fields from other index (mostly a relational typed index)
- Embedding   - Type that holds an embedded type (single instance) that appear as a property field inside the entity
- Nested      - Type that holds a nested type (possible for list of nexted elements) that appear as a property field/s inside the entity
- Partition   - Type that is partitioned according to some field (mostly time based) and is storde acros multiple partitions indices
 
These Abilities exist to provide the user tools to reflect the diverse use cases he may face.

Since each specific use-case may require different performance from the underlying system, the above options allow to express
these concerns in an explicit way.
 
 -------------

### Entities & Relations
 OpenGraph logical ontology allows the definition of three different categories:
  - entities
  - relationships
  - enumerations
  
#### Entities  
  Entities are a logical structure that represents an entity with its attributes. An entity may hold primitive data types such as
  integer fields, date fields, text fields and so...

#### In Addition

  Entity also may contain enumerated dictionary or even a subtype entity which is embedded inside the entity structure.

#### Example
  Example entity:
```json
        gender_enum: {
            Male,Female,Unknown    
        }

      Person: {
             id:  string
             name: string
             age:  int
             birth: date
             location: geo
             status: bool
             gender: gender_enum
             profession: Profession
           }
     
     Profession: {
            id: string
            name: string
            description: text
            since: date 
            salary:int
        }

```    
        
We can observe that Person has both simple primitive fields such as string, date, int but also complex fields such as the 
gender enumeration, the profession type fields and the geolocation struct.

-------------

### Schema Store
The 'opensearch' index provider offers the next possibilities to store (Index) Person / Profession entities : 
  
##### Static  
  Direct mapping of the entity type to a single index     
```json
       {
           "type": "Person",
           "partition": "static",  => shcematic mapping type
           "mapping": "Index",      
           "props": {
             "values": ["person"]  => this is the name of the physical index
           },
           "nested": [
             {
               "type": "Profession",  => this is the inner type belonging to the person entity
               "mapping": "child",    => inner type store as an embedded entity (other option in nested) 
               "partition": "nested",
               "props": {
                 "values": ["profession"]   => this is the name of the physical index
               }
             }
           ]
         }             
  ```   

  ##### Partitioned 
  Mapping of the entity type to a multiple indices where each index is called after some partitioned based field
  
 ```json
        {
           "type": "Person",    => this is the name of the physical index
           "partition": "time", => this is the partitioning type of the index
           "mapping":"Index",
           "symmetric":true
           ],
           "props": {
             "partition.field": "birth", => the partitioned field
             "prefix": "idx_person",     => the inedx common name
             "index.format": "idx_person_%s", => the incremental index naming pattern
             "date.format": "YYYY",           => the date format for the naming pattern
             "values": ["1900", "1950", "2000","2050"] => the indices incremental time buckets
           }
         }
  ```

  ##### Nested 
  Mapping the entity (sub)type to an index containing as an embedded/nested document 
  
  In this example the Profession is the nested entity here ...

```json
    {
      "type": "Person",
      "partition": "unified", => shcematic mapping type
      "mapping": "Index",
      "props": {
        "values": ["ontology"] => the unified index name
      },
      "nested": [
        {
          "type": "Profession",
          "mapping": "child", => "child" represents nested and "embedded" represents embedding the document insde the index
          "partition": "nested",
          "props": {
            "values": ["ontology"]
          }
        }
      ]
    }
```   

-------------

  #### Relationships  

  Relationships are a logical structure that represents a relationship between two entity with its attributes.
  A relationship may hold primitive data types such as integer fields, date fields, text fields and so...

  ##### Storing relationships  
  Relationships always connect two entities (this scope doesn't include start type relationships) and therefor identified
  by the two unique ids of each side of the relation. Let's take the former example with the two entities Person and Profession. 
  
  _Storage Options:_

If we would like to store this simple schema (Person,Profession) into opensearch - we can use one of the following index compositions:

1) Store each entity in a different Index: PersonIndex & ProfessionIndex and an additional index for the relationships (similar to an RSBMS noarmalized tables schema)
2) Store only one entity in an Index: PersonIndex & the second entity (Profession) will be nested inside that index - expressing mainly the first side of the relationship.
3) Store each entity in a different Index: PersonIndex & ProfessionIndex and each index will contain the other side of the relation as a nested document (relationship directionality has to be expressed as well).

While the first option is very typical to relational databased - it is tuned for space efficiency, the third option is typical to NoSql databased & it is tuned for search efficiency.

Another option that attempts to mitigate both concerns will be to create an index for each entity and store only the relation as a nested document.

 - Person will have an index with (in addition to its own fields) a nested relationship document containing only Profession ID & possibly some additional redundant field.
 - The other side of the relation (Profession) will also hold the skeleton version of the Person with the Id & some minimal basic fields (redundant fields representation).

This approach becomes highly efficient when there are **far more relationships than entities** and each side of the relation has many fields.

**_ redundant fields explained in the following section_**

```text
  ##########
  # Person #
  #  id    #
  #  name  #
  #  ....  #                                        ##################
  #  [ Profession ]    #                            #   Profession   #
  #             | Id   # -------------------------- #    ID          #
  #             | name #                            #    name        #
  ######################                            #    salary      #
                                                    #    .....       #
                                                    #   [Person]         #
                                                    #           | ID     #
                                                    #           | name   #
                                                    ######################
```  
Adding such capabilities extends the entities-relationship model implementations options and allow handling additional low-level optimization. 

---
  ##### Redundancy
  Redundancy is the ability to store redundant data on the relationship element that represents the information residing on either the side(s) of the relation.
  
  **Example:**
  
  Lets consider a 'Call' relationship type between two **person** entity 
 
  * **SideA** - is the left Side of the relationship  - a Person 
  * **SideB** - is the right Side of the relationship - a Person
  
  ```json
    {
        "type": "Call",
        "partition": "time", 
        "mapping":"Index",
        "symmetric":true,
        "redundant": [  => this section states which fields of the related entities are stored on the relation itself
          {
            "side":["entityA","entityB"], => indicate the side that the fields are taken from
            "redundant_name": "name",     => the field redundant name - in the relation index 
            "name": "name",               => the field original name - in the entity index
            "type": "string"              => the field type
          },
          {
            "side":["entityA","entityB"],
            "redundant_name": "number",
            "name": "number",
            "type": "string"
          }
        ],
        "props": {
          "partition.field": "date",
          "prefix": "idx_call",
          "index.format": "idx_call_%s",
          "date.format": "YYYY",
          "values": ["2001", "2002", "2003","2004"]
        }
      }
```

We are explicitly stating that name & number fields are saved (duplicated from their original index) for redundancy reasons to allow search acceleration.
The ability to query the same index for the 'remote' side of the relationship boosts performance on the expense of additional storage.
