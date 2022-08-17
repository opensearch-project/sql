# Ingestion, Normalization & Data Fusion

This RFC will introduce several methods and components that will help to normalize and consolidate ingested data into a concise and centralized schema.
It attempts to resolve the gap between the organized structure of the domain knowledge and the unstructured nature of the data.

---

This RFC will cover all the main steps of the data ingestion process including:

- **pre-processing**
- **matching**
    - schema matching
    - duplication detection
    - identity resolving
- **data fusion**

This section gives an overview of the functionality and the available different methods that are provided for each step...

``````
#  Pre-Processing  ->
#            -> schema matching  ->                       ->  Export Consolidated Scems
#                  - duplication detection
#                  - identity resolving
#                               -> data fusion -> 
#                                                            -> Emit fused records (entities)           
#

``````


## Data Model Definition

In order to utilize the full capability of the data - it must first be understood. Using a Schematic model
definition for the data allows this knowledge to be used in the different parts of the ingestion's process.

As proposed in RFC [OpenSearch Simple Schema](https://github.com/opensearch-project/sql/issues/763) using a schema
definition language to describe the Data model will allow the creation of dedicated API and generated code to be used in the next ingestion and processing
steps.

Using this data model for auto-generating indices in the storage layer will further amplify our normalization capabilities.

## Ingestion Pre-processing

During pre-processing phase data has to be prepared for the operations that will be applied upstream in the integration process.

Pre-processing techniques:

- Data type detection
- Units normalization
- Header detection ( for tables and CSV files)
- Subject detection (AKA Named Entity Resolution)

### Data Value Normalization

This step relates to the parsing of data values for various data types: boolean, URL, geo-point, date, numeric, string.
Input data values will be parsed, converted into the respective standard type, and normalised to a desired degree.

#### Numeric Data Types

For the data type numeric, further parsing of quantities (thousand, millions) and units of measurement (km, squareMile) is supported.
During normalisation, the quantities and units are handled by dedicated Numeric processors - for example scaling the value, i.e., normalising 3.5 km to 3500

#### Unit Normalizers

Defining the desired unit conversion is done by specifying a unit category such as Distance or Area.
The normaliser then checks for any unit belonging to this category and converts the value to the corresponding base unit

**Supported Unit Category**

- Frequency
- Area
- Volume
- Distance
- Power
- Speed
- Amount
- Time

#### Type Detection

There are cases in which the data-type of a field or an entity is unknown or not determined. For such cases we can try to
employ different types of detectors :

- Pattern based - can detect data types and units using a large range of regular expressions
- Context based - can detect data type and units using a context value based approach
- User defined -  this method allows the user to define general purpose rules for better customization

**_Web Based Scraping_**:

Web based content can be referred to as **_tables_** that were extracted or scraped from HTML pages, they may take form of
JSON files or CSV.

The formerly mentioned techniques will also be applied to this data source according to the next steps:

- Parse content - read the json/csv file into a table
- Detecting Data Types - determine the data types of the columns
- Detecting Entity Column / Subject Column - detecting a single column that likely contains the names of the entities that are described in a table.

## Schema Matching

The purpose of the Schema Matching phase is to reduce the total amount of variance and cardinality in the data.
By employing several techniques to compact both the amount of schemas and also collapsing allegedly different entities into one.

One of the results of schema matching is a mapping that allows the transformation of one schema into the other.

* Schemas can be aligned according to semantic correspondence between labels
* Schemas can be aligned according to value correspondence between fields
* Schemas can be aligned according to a known common value correspondence between records


### Identity Resolving
Identity resolving methods (also known as data matching or record linkage) identify records that describe the same real-world entity.

**_identity resolution_** methods can be applied to:
  - single dataset for **duplicate detection** 
  - multiple datasets in order to find **record-level correspondences** 

Identity resolution techniques rely on Indexing (blocking) in order to reduce the number of record to compare.

Optional Indexing methods:
  - Indexing by single/multiple blocking key(s)
  - Sorted-Neighbourhood Method
  - Rule-based identity resolution

The **_Matching Processor_**   

For Schema Matching and Identity Resolution, a _**Matching processor**_ will provide a starting point for many common operations.
Data is stored in logical Sets, which can contain both a schema and a list of records (entities). These sets are than submitted to the Matching processor.

The following methods are employed to create a corresponding measurement for each record in the set:

- Indexing methods for records
  linkage [Blocking Generators](http://users.cecs.anu.edu.au/~christen/publications/kdd03-6pages.pdf)
    - Run blocking transformation to reduce the amount of compartments
- Matching rule runs on top of the generated blocks - for each a pair of records -> determines if they are matching or not.
    - Run Different types of matching rules 
      - TODO - describe matching rules
    - Calculate the cumulative match score

### Identity Matching Process

After defining the domain entities, in the process of identity resolution, records of two datasets are compared using a
matching rule. The rule predict with a measure of certainty whether two records describe the same real-world entity or
not.

**_Strategies for Matching_**:

- Linear Combination of Field values - each field has a distance measure according to its type:
    - String : Levenshtein distance
    - Numbers: Harmonic / Gaussian distance
    - Dates :  _TODO_
    - Geo   :  _TODO_

- ML based matching rule using a pre-trained [classifier](http://weka.sourceforge.net/doc.dev/weka/classifiers/Classifier.html)

The result of this step is a set of correspondences containing the IDs of the matching record (entities) pairs.

## Data Fusion

Once the former steps are performed, we have a unified schema with a tuple of scored records (entities) matching.
From these tuples of record matching - we will generate a correspondences set which contains all the "same entities"
records with their corresponding attribute values.

We will need to select a single value that will best match the actual meaning of that entity's field. Applying a set of field
based rules to resolve the difference's between optional field values:

- **Majority rule** - simply take the most common field value
- **Average/Median rule** - selects the average/median value
- **Cluster rule** - cluster the values with the similarity measure and selecting the centroid
- **Intersection rule** - selects the intersection of all values
- **Customized rule** - custom user based rule (optional to composite different rules)

## Conclusion

As described above, these set of steps will transform unstructured or semi-structured data into a coherent set of entities.
Adding these capabilities to the new ([OpenSearch Simple Schema](https://github.com/opensearch-project/sql/issues/763)) schematic structure support of opensearch
- will result in a knowledge oriented system that offers advanced tools for Ingestion and organizing the data.

The stream nature of these methods is idle for placing them in the ingestion step prior to storage in opensearch. Nevertheless, these methods
can also be applied in a bach like manner on existing indices.

_**It's important to emphasize that while these heuristic techniques can give a good score (in terms of Precision, F1-Score & Recalls) - data is neglected during the process.**_ 
