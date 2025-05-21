/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


parser grammar OpenSearchPPLParser;


options { tokenVocab = OpenSearchPPLLexer; }
root
   : pplStatement? EOF
   ;

// statement
pplStatement
   : dmlStatement
   ;

dmlStatement
   : queryStatement
   | explainStatement
   ;

queryStatement
   : pplCommands (PIPE commands)*
   ;

explainStatement
    : EXPLAIN (explainMode)? queryStatement
    ;

explainMode
    : SIMPLE
    | STANDARD
    | COST
    | EXTENDED
    ;

subSearch
   : searchCommand (PIPE commands)*
   ;

// commands
pplCommands
   : searchCommand
   | describeCommand
   | showDataSourcesCommand
   ;

commands
   : whereCommand
   | fieldsCommand
   | joinCommand
   | renameCommand
   | statsCommand
   | eventstatsCommand
   | dedupCommand
   | sortCommand
   | evalCommand
   | headCommand
   | topCommand
   | rareCommand
   | grokCommand
   | parseCommand
   | patternsCommand
   | lookupCommand
   | kmeansCommand
   | adCommand
   | mlCommand
   | fillnullCommand
   | trendlineCommand
   ;

commandName
   : SEARCH
   | DESCRIBE
   | SHOW
   | WHERE
   | FIELDS
   | JOIN
   | RENAME
   | STATS
   | EVENTSTATS
   | DEDUP
   | SORT
   | EVAL
   | HEAD
   | TOP
   | RARE
   | GROK
   | PARSE
   | PATTERNS
   | LOOKUP
   | KMEANS
   | AD
   | ML
   | FILLNULL
   | TRENDLINE
   | EXPLAIN
   ;

searchCommand
   : (SEARCH)? fromClause                       # searchFrom
   | (SEARCH)? fromClause logicalExpression     # searchFromFilter
   | (SEARCH)? logicalExpression fromClause     # searchFilterFrom
   ;

describeCommand
   : DESCRIBE tableSourceClause
   ;

showDataSourcesCommand
   : SHOW DATASOURCES
   ;

whereCommand
   : WHERE logicalExpression
   ;

fieldsCommand
   : FIELDS (PLUS | MINUS)? fieldList
   ;

renameCommand
   : RENAME renameClasue (COMMA renameClasue)*
   ;

statsCommand
   : STATS (PARTITIONS EQUAL partitions = integerLiteral)? (ALLNUM EQUAL allnum = booleanLiteral)? (DELIM EQUAL delim = stringLiteral)? statsAggTerm (COMMA statsAggTerm)* (statsByClause)? (DEDUP_SPLITVALUES EQUAL dedupsplit = booleanLiteral)?
   ;

eventstatsCommand
   : EVENTSTATS eventstatsAggTerm (COMMA eventstatsAggTerm)* (statsByClause)?
   ;

dedupCommand
   : DEDUP (number = integerLiteral)? fieldList (KEEPEMPTY EQUAL keepempty = booleanLiteral)? (CONSECUTIVE EQUAL consecutive = booleanLiteral)?
   ;

sortCommand
   : SORT sortbyClause
   ;

evalCommand
   : EVAL evalClause (COMMA evalClause)*
   ;

headCommand
   : HEAD (number = integerLiteral)? (FROM from = integerLiteral)?
   ;

topCommand
   : TOP (number = integerLiteral)? fieldList (byClause)?
   ;

rareCommand
   : RARE (number = integerLiteral)? fieldList (byClause)?
   ;

grokCommand
   : GROK (source_field = expression) (pattern = stringLiteral)
   ;

parseCommand
   : PARSE (source_field = expression) (pattern = stringLiteral)
   ;

patternsMethod
   : PUNCT
   | REGEX
   ;

patternsCommand
   : PATTERNS (patternsParameter)* (source_field = expression) (pattern_method = patternMethod)*
   ;

patternsParameter
   : (NEW_FIELD EQUAL new_field = stringLiteral)
   | (PATTERN EQUAL pattern = stringLiteral)
   | (VARIABLE_COUNT_THRESHOLD EQUAL variable_count_threshold = integerLiteral)
   | (FREQUENCY_THRESHOLD_PERCENTAGE EQUAL frequency_threshold_percentage = decimalLiteral)
   ;

patternMethod
   : SIMPLE_PATTERN
   | BRAIN
   ;

// lookup
lookupCommand
   : LOOKUP tableSource lookupMappingList ((APPEND | REPLACE) outputCandidateList)?
   ;

lookupMappingList
   : lookupPair (COMMA lookupPair)*
   ;

outputCandidateList
   : lookupPair (COMMA lookupPair)*
   ;

 // The lookup pair will generate a K-V pair.
 // The format is Key -> Alias(outputFieldName, inputField), Value -> outputField. For example:
 // 1. When lookupPair is "name AS cName", the key will be Alias(cName, Field(name)), the value will be Field(cName)
 // 2. When lookupPair is "dept", the key is Alias(dept, Field(dept)), value is Field(dept)
lookupPair
   : inputField = fieldExpression (AS outputField = fieldExpression)?
   ;

fillnullCommand
   : FILLNULL (fillNullWithTheSameValue
   | fillNullWithFieldVariousValues)
   ;

fillNullWithTheSameValue
   : WITH nullReplacement = valueExpression IN nullableFieldList = fieldList
   ;

fillNullWithFieldVariousValues
   : USING nullReplacementExpression (COMMA nullReplacementExpression)*
   ;

nullReplacementExpression
   : nullableField = fieldExpression EQUAL nullReplacement = valueExpression
   ;

trendlineCommand
   : TRENDLINE (SORT sortField)? trendlineClause (trendlineClause)*
   ;

trendlineClause
   : trendlineType LT_PRTHS numberOfDataPoints = integerLiteral COMMA field = fieldExpression RT_PRTHS (AS alias = qualifiedName)?
   ;

trendlineType
   : SMA
   ;

kmeansCommand
   : KMEANS (kmeansParameter)*
   ;

kmeansParameter
   : (CENTROIDS EQUAL centroids = integerLiteral)
   | (ITERATIONS EQUAL iterations = integerLiteral)
   | (DISTANCE_TYPE EQUAL distance_type = stringLiteral)
   ;

adCommand
   : AD (adParameter)*
   ;

adParameter
   : (NUMBER_OF_TREES EQUAL number_of_trees = integerLiteral)
   | (SHINGLE_SIZE EQUAL shingle_size = integerLiteral)
   | (SAMPLE_SIZE EQUAL sample_size = integerLiteral)
   | (OUTPUT_AFTER EQUAL output_after = integerLiteral)
   | (TIME_DECAY EQUAL time_decay = decimalLiteral)
   | (ANOMALY_RATE EQUAL anomaly_rate = decimalLiteral)
   | (CATEGORY_FIELD EQUAL category_field = stringLiteral)
   | (TIME_FIELD EQUAL time_field = stringLiteral)
   | (DATE_FORMAT EQUAL date_format = stringLiteral)
   | (TIME_ZONE EQUAL time_zone = stringLiteral)
   | (TRAINING_DATA_SIZE EQUAL training_data_size = integerLiteral)
   | (ANOMALY_SCORE_THRESHOLD EQUAL anomaly_score_threshold = decimalLiteral)
   ;

mlCommand
   : ML (mlArg)*
   ;

mlArg
   : (argName = ident EQUAL argValue = literalValue)
   ;

// clauses
fromClause
   : SOURCE EQUAL tableOrSubqueryClause
   | INDEX EQUAL tableOrSubqueryClause
   | SOURCE EQUAL tableFunction
   | INDEX EQUAL tableFunction
   ;

tableOrSubqueryClause
   : LT_SQR_PRTHS subSearch RT_SQR_PRTHS (AS alias = qualifiedName)?
   | tableSourceClause
   ;

tableSourceClause
   : tableSource (COMMA tableSource)* (AS alias = qualifiedName)?
   ;

// join
joinCommand
   : (joinType) JOIN sideAlias joinHintList? joinCriteria? right = tableOrSubqueryClause
   ;

joinType
   : INNER?
   | CROSS
   | LEFT OUTER?
   | RIGHT OUTER?
   | FULL OUTER?
   | LEFT? SEMI
   | LEFT? ANTI
   ;

sideAlias
   : (LEFT EQUAL leftAlias = qualifiedName)? COMMA? (RIGHT EQUAL rightAlias = qualifiedName)?
   ;

joinCriteria
   : ON logicalExpression
   ;

joinHintList
   : hintPair (COMMA? hintPair)*
   ;

hintPair
   : leftHintKey = LEFT_HINT DOT ID EQUAL leftHintValue = ident             #leftHint
   | rightHintKey = RIGHT_HINT DOT ID EQUAL rightHintValue = ident          #rightHint
   ;

renameClasue
   : orignalField = wcFieldExpression AS renamedField = wcFieldExpression
   ;

byClause
   : BY fieldList
   ;

statsByClause
   : BY fieldList
   | BY bySpanClause
   | BY bySpanClause COMMA fieldList
   | BY fieldList COMMA bySpanClause
   ;

bySpanClause
   : spanClause (AS alias = qualifiedName)?
   ;

spanClause
   : SPAN LT_PRTHS fieldExpression COMMA value = literalValue (unit = timespanUnit)? RT_PRTHS
   ;

sortbyClause
   : sortField (COMMA sortField)*
   ;

evalClause
   : fieldExpression EQUAL expression
   ;

eventstatsAggTerm
   : windowFunction (AS alias = wcFieldExpression)?
   ;

windowFunction
   : windowFunctionName LT_PRTHS functionArgs RT_PRTHS
   ;

windowFunctionName
   : statsFunctionName
   | scalarWindowFunctionName
   ;

scalarWindowFunctionName
   : ROW_NUMBER
   | RANK
   | DENSE_RANK
   | PERCENT_RANK
   | CUME_DIST
   | FIRST
   | LAST
   | NTH
   | NTILE
   ;

// aggregation terms
statsAggTerm
   : statsFunction (AS alias = wcFieldExpression)?
   ;

// aggregation functions
statsFunction
   : statsFunctionName LT_PRTHS valueExpression RT_PRTHS        # statsFunctionCall
   | COUNT LT_PRTHS RT_PRTHS                                    # countAllFunctionCall
   | (DISTINCT_COUNT | DC | DISTINCT_COUNT_APPROX) LT_PRTHS valueExpression RT_PRTHS    # distinctCountFunctionCall
   | takeAggFunction                                            # takeAggFunctionCall
   | percentileApproxFunction                                   # percentileApproxFunctionCall
   ;

statsFunctionName
   : AVG
   | COUNT
   | SUM
   | MIN
   | MAX
   | VAR_SAMP
   | VAR_POP
   | STDDEV_SAMP
   | STDDEV_POP
   | PERCENTILE
   | PERCENTILE_APPROX
   ;

takeAggFunction
   : TAKE LT_PRTHS fieldExpression (COMMA size = integerLiteral)? RT_PRTHS
   ;

percentileApproxFunction
   : (PERCENTILE | PERCENTILE_APPROX) LT_PRTHS aggField = valueExpression
       COMMA percent = numericLiteral (COMMA compression = numericLiteral)? RT_PRTHS
   ;

numericLiteral
    : integerLiteral
    | decimalLiteral
    ;

// expressions
expression
   : logicalExpression
   | comparisonExpression
   | valueExpression
   ;

// predicates
logicalExpression
   : LT_PRTHS logicalExpression RT_PRTHS                        # parentheticLogicalExpr
   | NOT logicalExpression                                      # logicalNot
   | left = logicalExpression (AND)? right = logicalExpression  # logicalAnd
   | left = logicalExpression XOR right = logicalExpression     # logicalXor
   | left = logicalExpression OR right = logicalExpression      # logicalOr
   | comparisonExpression                                       # comparsion
   | booleanExpression                                          # booleanExpr
   | relevanceExpression                                        # relevanceExpr
   ;

comparisonExpression
   : left = valueExpression comparisonOperator right = valueExpression      # compareExpr
   | valueExpression NOT? IN valueList                                      # inExpr
   | valueExpression NOT? BETWEEN valueExpression AND valueExpression       # between
   ;

valueExpressionList
   : valueExpression
   | LT_PRTHS valueExpression (COMMA valueExpression)* RT_PRTHS
   ;

valueExpression
   : left = valueExpression binaryOperator = (STAR | DIVIDE | MODULE) right = valueExpression   # binaryArithmetic
   | left = valueExpression binaryOperator = (PLUS | MINUS) right = valueExpression             # binaryArithmetic
   | primaryExpression                                                                          # valueExpressionDefault
   | positionFunction                                                                           # positionFunctionCall
   | caseFunction                                                                               # caseExpr
   | extractFunction                                                                            # extractFunctionCall
   | getFormatFunction                                                                          # getFormatFunctionCall
   | timestampFunction                                                                          # timestampFunctionCall
   | LT_PRTHS valueExpression RT_PRTHS                                                          # parentheticValueExpr
   | LT_SQR_PRTHS subSearch RT_SQR_PRTHS                                                        # scalarSubqueryExpr
   ;

primaryExpression
   : evalFunctionCall
   | dataTypeFunctionCall
   | fieldExpression
   | literalValue
   ;

positionFunction
   : positionFunctionName LT_PRTHS functionArg IN functionArg RT_PRTHS
   ;

booleanExpression
   : booleanFunctionCall                                                # booleanFunctionCallExpr
   | valueExpressionList NOT? IN LT_SQR_PRTHS subSearch RT_SQR_PRTHS    # inSubqueryExpr
   | EXISTS LT_SQR_PRTHS subSearch RT_SQR_PRTHS                         # existsSubqueryExpr
   ;

caseFunction
   : CASE LT_PRTHS logicalExpression COMMA valueExpression (COMMA logicalExpression COMMA valueExpression)* (ELSE valueExpression)? RT_PRTHS
   ;

relevanceExpression
   : singleFieldRelevanceFunction
   | multiFieldRelevanceFunction
   ;

// Field is a single column
singleFieldRelevanceFunction
   : singleFieldRelevanceFunctionName LT_PRTHS field = relevanceField COMMA query = relevanceQuery (COMMA relevanceArg)* RT_PRTHS
   ;

// Field is a list of columns
multiFieldRelevanceFunction
   : multiFieldRelevanceFunctionName LT_PRTHS LT_SQR_PRTHS field = relevanceFieldAndWeight (COMMA field = relevanceFieldAndWeight)* RT_SQR_PRTHS COMMA query = relevanceQuery (COMMA relevanceArg)* RT_PRTHS
   ;

// tables
tableSource
   : tableQualifiedName
   | ID_DATE_SUFFIX
   ;

tableFunction
   : qualifiedName LT_PRTHS functionArgs RT_PRTHS
   ;

// fields
fieldList
   : fieldExpression (COMMA fieldExpression)*
   ;

wcFieldList
   : wcFieldExpression (COMMA wcFieldExpression)*
   ;

sortField
   : (PLUS | MINUS)? sortFieldExpression
   ;

sortFieldExpression
   : fieldExpression
   | AUTO LT_PRTHS fieldExpression RT_PRTHS
   | STR LT_PRTHS fieldExpression RT_PRTHS
   | IP LT_PRTHS fieldExpression RT_PRTHS
   | NUM LT_PRTHS fieldExpression RT_PRTHS
   ;

fieldExpression
   : qualifiedName
   ;

wcFieldExpression
   : wcQualifiedName
   ;

// functions
evalFunctionCall
   : evalFunctionName LT_PRTHS functionArgs RT_PRTHS
   ;

// cast function
dataTypeFunctionCall
   : CAST LT_PRTHS expression AS convertedDataType RT_PRTHS
   ;

// boolean functions
booleanFunctionCall
   : conditionFunctionName LT_PRTHS functionArgs RT_PRTHS
   ;

convertedDataType
   : typeName = DATE
   | typeName = TIME
   | typeName = TIMESTAMP
   | typeName = INT
   | typeName = INTEGER
   | typeName = DOUBLE
   | typeName = LONG
   | typeName = FLOAT
   | typeName = STRING
   | typeName = BOOLEAN
   | typeName = IP
   | typeName = JSON
   ;

evalFunctionName
   : mathematicalFunctionName
   | dateTimeFunctionName
   | textFunctionName
   | conditionFunctionName
   | flowControlFunctionName
   | systemFunctionName
   | positionFunctionName
   | cryptographicFunctionName
   | jsonFunctionName
   | geoipFunctionName
   ;

functionArgs
   : (functionArg (COMMA functionArg)*)?
   ;

functionArg
   : (ident EQUAL)? expression
   ;

relevanceArg
   : relevanceArgName EQUAL relevanceArgValue
   ;

relevanceArgName
   : ALLOW_LEADING_WILDCARD
   | ANALYZER
   | ANALYZE_WILDCARD
   | AUTO_GENERATE_SYNONYMS_PHRASE_QUERY
   | BOOST
   | CUTOFF_FREQUENCY
   | DEFAULT_FIELD
   | DEFAULT_OPERATOR
   | ENABLE_POSITION_INCREMENTS
   | ESCAPE
   | FIELDS
   | FLAGS
   | FUZZINESS
   | FUZZY_MAX_EXPANSIONS
   | FUZZY_PREFIX_LENGTH
   | FUZZY_REWRITE
   | FUZZY_TRANSPOSITIONS
   | LENIENT
   | LOW_FREQ_OPERATOR
   | MAX_DETERMINIZED_STATES
   | MAX_EXPANSIONS
   | MINIMUM_SHOULD_MATCH
   | OPERATOR
   | PHRASE_SLOP
   | PREFIX_LENGTH
   | QUOTE_ANALYZER
   | QUOTE_FIELD_SUFFIX
   | REWRITE
   | SLOP
   | TIE_BREAKER
   | TIME_ZONE
   | TYPE
   | ZERO_TERMS_QUERY
   ;

relevanceFieldAndWeight
   : field = relevanceField
   | field = relevanceField weight = relevanceFieldWeight
   | field = relevanceField BIT_XOR_OP weight = relevanceFieldWeight
   ;

relevanceFieldWeight
   : integerLiteral
   | decimalLiteral
   ;

relevanceField
   : qualifiedName
   | stringLiteral
   ;

relevanceQuery
   : relevanceArgValue
   ;

relevanceArgValue
   : qualifiedName
   | literalValue
   ;

mathematicalFunctionName
   : ABS
   | CBRT
   | CEIL
   | CEILING
   | CONV
   | CRC32
   | E
   | EXP
   | FLOOR
   | LN
   | LOG
   | LOG10
   | LOG2
   | MOD
   | PI
   | POW
   | POWER
   | RAND
   | ROUND
   | SIGN
   | SQRT
   | TRUNCATE
   | trigonometricFunctionName
   ;

geoipFunctionName
   : GEOIP
   ;

trigonometricFunctionName
   : ACOS
   | ASIN
   | ATAN
   | ATAN2
   | COS
   | COT
   | DEGREES
   | RADIANS
   | SIN
   | TAN
   ;

cryptographicFunctionName
   : MD5
   | SHA1
   | SHA2
   ;

dateTimeFunctionName
   : ADDDATE
   | ADDTIME
   | CONVERT_TZ
   | CURDATE
   | CURRENT_DATE
   | CURRENT_TIME
   | CURRENT_TIMESTAMP
   | CURTIME
   | DATE
   | DATEDIFF
   | DATETIME
   | DATE_ADD
   | DATE_FORMAT
   | DATE_SUB
   | DAY
   | DAYNAME
   | DAYOFMONTH
   | DAYOFWEEK
   | DAYOFYEAR
   | DAY_OF_MONTH
   | DAY_OF_WEEK
   | DAY_OF_YEAR
   | FROM_DAYS
   | FROM_UNIXTIME
   | HOUR
   | HOUR_OF_DAY
   | LAST_DAY
   | LOCALTIME
   | LOCALTIMESTAMP
   | MAKEDATE
   | MAKETIME
   | MICROSECOND
   | MINUTE
   | MINUTE_OF_DAY
   | MINUTE_OF_HOUR
   | MONTH
   | MONTHNAME
   | MONTH_OF_YEAR
   | NOW
   | PERIOD_ADD
   | PERIOD_DIFF
   | QUARTER
   | SECOND
   | SECOND_OF_MINUTE
   | SEC_TO_TIME
   | STR_TO_DATE
   | SUBDATE
   | SUBTIME
   | SYSDATE
   | TIME
   | TIMEDIFF
   | TIMESTAMP
   | TIME_FORMAT
   | TIME_TO_SEC
   | TO_DAYS
   | TO_SECONDS
   | UNIX_TIMESTAMP
   | UTC_DATE
   | UTC_TIME
   | UTC_TIMESTAMP
   | WEEK
   | WEEKDAY
   | WEEK_OF_YEAR
   | YEAR
   | YEARWEEK
   ;

getFormatFunction
   : GET_FORMAT LT_PRTHS getFormatType COMMA functionArg RT_PRTHS
   ;

getFormatType
   : DATE
   | DATETIME
   | TIME
   | TIMESTAMP
   ;

extractFunction
   : EXTRACT LT_PRTHS datetimePart FROM functionArg RT_PRTHS
   ;

simpleDateTimePart
   : MICROSECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   ;

complexDateTimePart
   : SECOND_MICROSECOND
   | MINUTE_MICROSECOND
   | MINUTE_SECOND
   | HOUR_MICROSECOND
   | HOUR_SECOND
   | HOUR_MINUTE
   | DAY_MICROSECOND
   | DAY_SECOND
   | DAY_MINUTE
   | DAY_HOUR
   | YEAR_MONTH
   ;

datetimePart
   : simpleDateTimePart
   | complexDateTimePart
   ;

timestampFunction
   : timestampFunctionName LT_PRTHS simpleDateTimePart COMMA firstArg = functionArg COMMA secondArg = functionArg RT_PRTHS
   ;

timestampFunctionName
   : TIMESTAMPADD
   | TIMESTAMPDIFF
   ;

// condition function return boolean value
conditionFunctionName
   : LIKE
   | ISNULL
   | ISNOTNULL
   | CIDRMATCH
   | JSON_VALID
   ;

// flow control function return non-boolean value
flowControlFunctionName
   : IF
   | IFNULL
   | NULLIF
   ;

systemFunctionName
   : TYPEOF
   ;

textFunctionName
   : SUBSTR
   | SUBSTRING
   | TRIM
   | LTRIM
   | RTRIM
   | LOWER
   | UPPER
   | CONCAT
   | CONCAT_WS
   | LENGTH
   | STRCMP
   | RIGHT
   | LEFT
   | ASCII
   | LOCATE
   | REPLACE
   | REVERSE
   ;

positionFunctionName
   : POSITION
   ;

jsonFunctionName
   : JSON
   ;

// operators
 comparisonOperator
   : EQUAL
   | NOT_EQUAL
   | LESS
   | NOT_LESS
   | GREATER
   | NOT_GREATER
   | REGEXP
   ;

singleFieldRelevanceFunctionName
   : MATCH
   | MATCH_PHRASE
   | MATCH_BOOL_PREFIX
   | MATCH_PHRASE_PREFIX
   ;

multiFieldRelevanceFunctionName
   : SIMPLE_QUERY_STRING
   | MULTI_MATCH
   | QUERY_STRING
   ;

// literals and values
literalValue
   : intervalLiteral
   | stringLiteral
   | integerLiteral
   | decimalLiteral
   | booleanLiteral
   | datetimeLiteral //#datetime
   ;

intervalLiteral
   : INTERVAL valueExpression intervalUnit
   ;

stringLiteral
   : DQUOTA_STRING
   | SQUOTA_STRING
   ;

integerLiteral
   : (PLUS | MINUS)? INTEGER_LITERAL
   ;

decimalLiteral
   : (PLUS | MINUS)? DECIMAL_LITERAL
   ;

booleanLiteral
   : TRUE
   | FALSE
   ;

// Date and Time Literal, follow ANSI 92
datetimeLiteral
   : dateLiteral
   | timeLiteral
   | timestampLiteral
   ;

dateLiteral
   : DATE date = stringLiteral
   ;

timeLiteral
   : TIME time = stringLiteral
   ;

timestampLiteral
   : TIMESTAMP timestamp = stringLiteral
   ;

intervalUnit
   : MICROSECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   | SECOND_MICROSECOND
   | MINUTE_MICROSECOND
   | MINUTE_SECOND
   | HOUR_MICROSECOND
   | HOUR_SECOND
   | HOUR_MINUTE
   | DAY_MICROSECOND
   | DAY_SECOND
   | DAY_MINUTE
   | DAY_HOUR
   | YEAR_MONTH
   ;

timespanUnit
   : MS
   | S
   | M
   | H
   | D
   | W
   | Q
   | Y
   | MILLISECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   ;

valueList
   : LT_PRTHS literalValue (COMMA literalValue)* RT_PRTHS
   ;

qualifiedName
   : ident (DOT ident)* # identsAsQualifiedName
   ;

tableQualifiedName
   : tableIdent (DOT ident)* # identsAsTableQualifiedName
   ;

wcQualifiedName
   : wildcard (DOT wildcard)* # identsAsWildcardQualifiedName
   ;

ident
   : (DOT)? ID
   | BACKTICK ident BACKTICK
   | BQUOTA_STRING
   | keywordsCanBeId
   ;

tableIdent
   : (CLUSTER)? ident
   ;

wildcard
   : ident (MODULE ident)* (MODULE)?
   | SINGLE_QUOTE wildcard SINGLE_QUOTE
   | DOUBLE_QUOTE wildcard DOUBLE_QUOTE
   | BACKTICK wildcard BACKTICK
   ;

keywordsCanBeId
   : D // OD SQL and ODBC special
   | timespanUnit
   | SPAN
   | evalFunctionName
   | relevanceArgName
   | intervalUnit
   | trendlineType
   | singleFieldRelevanceFunctionName
   | multiFieldRelevanceFunctionName
   | commandName
   | comparisonOperator
   | patternMethod
   | explainMode
   // commands assist keywords
   | CASE
   | ELSE
   | IN
   | BETWEEN
   | EXISTS
   | SOURCE
   | INDEX
   | DESC
   | DATASOURCES
   | FROM
   | PATTERN
   | NEW_FIELD
   | VARIABLE_COUNT_THRESHOLD
   | FREQUENCY_THRESHOLD_PERCENTAGE
   | WITH
   | REGEX
   | PUNCT
   | USING
   | CAST
   | GET_FORMAT
   | EXTRACT
   | INTERVAL
   | PLUS
   | MINUS
   // SORT FIELD KEYWORDS
   | AUTO
   | STR
   | IP
   | NUM
   // ARGUMENT KEYWORDS
   | KEEPEMPTY
   | CONSECUTIVE
   | DEDUP_SPLITVALUES
   | PARTITIONS
   | ALLNUM
   | DELIM
   | CENTROIDS
   | ITERATIONS
   | DISTANCE_TYPE
   | NUMBER_OF_TREES
   | SHINGLE_SIZE
   | SAMPLE_SIZE
   | OUTPUT_AFTER
   | TIME_DECAY
   | ANOMALY_RATE
   | CATEGORY_FIELD
   | TIME_FIELD
   | TIME_ZONE
   | TRAINING_DATA_SIZE
   | ANOMALY_SCORE_THRESHOLD
   // AGGREGATIONS AND WINDOW
   | statsFunctionName
   | windowFunctionName
   | DISTINCT_COUNT
   | DISTINCT_COUNT_APPROX
   | ESTDC
   | ESTDC_ERROR
   | MEAN
   | MEDIAN
   | MODE
   | RANGE
   | STDEV
   | STDEVP
   | SUMSQ
   | VAR_SAMP
   | VAR_POP
   | TAKE
   | LIST
   | VALUES
   | PER_DAY
   | PER_HOUR
   | PER_MINUTE
   | PER_SECOND
   | RATE
   | SPARKLINE
   | C
   | DC
   // JOIN TYPE
   | OUTER
   | INNER
   | CROSS
   | LEFT
   | RIGHT
   | FULL
   | SEMI
   | ANTI
   | LEFT_HINT
   | RIGHT_HINT
   ;
