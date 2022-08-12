/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


lexer grammar OpenSearchPPLLexer;

channels { WHITESPACE, ERRORCHANNEL }


// COMMAND KEYWORDS
SEARCH:                             'SEARCH';
DESCRIBE:                           'DESCRIBE';
FROM:                               'FROM';
WHERE:                              'WHERE';
FIELDS:                             'FIELDS';
RENAME:                             'RENAME';
STATS:                              'STATS';
DEDUP:                              'DEDUP';
SORT:                               'SORT';
EVAL:                               'EVAL';
HEAD:                               'HEAD';
TOP:                                'TOP';
RARE:                               'RARE';
PARSE:                              'PARSE';
KMEANS:                             'KMEANS';
AD:                                 'AD';

// COMMAND ASSIST KEYWORDS
AS:                                 'AS';
BY:                                 'BY';
SOURCE:                             'SOURCE';
INDEX:                              'INDEX';
D:                                  'D';
DESC:                               'DESC';

// CLAUSE KEYWORDS
SORTBY:                             'SORTBY';

// FIELD KEYWORDS
AUTO:                               'AUTO';
STR:                                'STR';
IP:                                 'IP';
NUM:                                'NUM';

// ARGUMENT KEYWORDS
KEEPEMPTY:                          'KEEPEMPTY';
CONSECUTIVE:                        'CONSECUTIVE';
DEDUP_SPLITVALUES:                  'DEDUP_SPLITVALUES';
PARTITIONS:                         'PARTITIONS';
ALLNUM:                             'ALLNUM';
DELIM:                              'DELIM';
CENTROIDS:                          'CENTROIDS';
ITERATIONS:                         'ITERATIONS';
DISTANCE_TYPE:                      'DISTANCE_TYPE';
NUMBER_OF_TREES:                    'NUMBER_OF_TREES';
SHINGLE_SIZE:                       'SHINGLE_SIZE';
SAMPLE_SIZE:                        'SAMPLE_SIZE';
OUTPUT_AFTER:                       'OUTPUT_AFTER';
TIME_DECAY:                         'TIME_DECAY';
ANOMALY_RATE:                       'ANOMALY_RATE';
TIME_FIELD:                         'TIME_FIELD';
TIME_ZONE:                          'TIME_ZONE';
TRAINING_DATA_SIZE:                 'TRAINING_DATA_SIZE';
ANOMALY_SCORE_THRESHOLD:            'ANOMALY_SCORE_THRESHOLD';

// COMPARISON FUNCTION KEYWORDS
CASE:                               'CASE';
IN:                                 'IN';

// LOGICAL KEYWORDS
NOT:                                'NOT';
OR:                                 'OR';
AND:                                'AND';
XOR:                                'XOR';
TRUE:                               'TRUE';
FALSE:                              'FALSE';
REGEXP:                             'REGEXP';

// DATETIME, INTERVAL AND UNIT KEYWORDS
DATETIME:                           'DATETIME';
INTERVAL:                           'INTERVAL';
MICROSECOND:                        'MICROSECOND';
MILLISECOND:                        'MILLISECOND';
SECOND:                             'SECOND';
MINUTE:                             'MINUTE';
HOUR:                               'HOUR';
DAY:                                'DAY';
WEEK:                               'WEEK';
MONTH:                              'MONTH';
QUARTER:                            'QUARTER';
YEAR:                               'YEAR';
SECOND_MICROSECOND:                 'SECOND_MICROSECOND';
MINUTE_MICROSECOND:                 'MINUTE_MICROSECOND';
MINUTE_SECOND:                      'MINUTE_SECOND';
HOUR_MICROSECOND:                   'HOUR_MICROSECOND';
HOUR_SECOND:                        'HOUR_SECOND';
HOUR_MINUTE:                        'HOUR_MINUTE';
DAY_MICROSECOND:                    'DAY_MICROSECOND';
DAY_SECOND:                         'DAY_SECOND';
DAY_MINUTE:                         'DAY_MINUTE';
DAY_HOUR:                           'DAY_HOUR';
YEAR_MONTH:                         'YEAR_MONTH';

// DATASET TYPES
DATAMODEL:                          'DATAMODEL';
LOOKUP:                             'LOOKUP';
SAVEDSEARCH:                        'SAVEDSEARCH';

// CONVERTED DATA TYPES
INT:                                'INT';
INTEGER:                            'INTEGER';
DOUBLE:                             'DOUBLE';
LONG:                               'LONG';
FLOAT:                              'FLOAT';
STRING:                             'STRING';
BOOLEAN:                            'BOOLEAN';

// SPECIAL CHARACTERS AND OPERATORS
PIPE:                               '|';
COMMA:                              ',';
DOT:                                '.';
EQUAL:                              '=';
GREATER:                            '>';
LESS:                               '<';
NOT_GREATER:                        '<' '=';
NOT_LESS:                           '>' '=';
NOT_EQUAL:                          '!' '=';
PLUS:                               '+';
MINUS:                              '-';
STAR:                               '*';
DIVIDE:                             '/';
MODULE:                             '%';
EXCLAMATION_SYMBOL:                 '!';
COLON:                              ':';
LT_PRTHS:                           '(';
RT_PRTHS:                           ')';
LT_SQR_PRTHS:                       '[';
RT_SQR_PRTHS:                       ']';
SINGLE_QUOTE:                       '\'';
DOUBLE_QUOTE:                       '"';
BACKTICK:                           '`';

// Operators. Bit

BIT_NOT_OP:                         '~';
BIT_AND_OP:                         '&';
BIT_XOR_OP:                         '^';

// AGGREGATIONS
AVG:                                'AVG';
COUNT:                              'COUNT';
DISTINCT_COUNT:                     'DISTINCT_COUNT';
ESTDC:                              'ESTDC';
ESTDC_ERROR:                        'ESTDC_ERROR';
MAX:                                'MAX';
MEAN:                               'MEAN';
MEDIAN:                             'MEDIAN';
MIN:                                'MIN';
MODE:                               'MODE';
RANGE:                              'RANGE';
STDEV:                              'STDEV';
STDEVP:                             'STDEVP';
SUM:                                'SUM';
SUMSQ:                              'SUMSQ';
VAR_SAMP:                           'VAR_SAMP';
VAR_POP:                            'VAR_POP';
STDDEV_SAMP:                        'STDDEV_SAMP';
STDDEV_POP:                         'STDDEV_POP';
PERCENTILE:                         'PERCENTILE';
FIRST:                              'FIRST';
LAST:                               'LAST';
LIST:                               'LIST';
VALUES:                             'VALUES';
EARLIEST:                           'EARLIEST';
EARLIEST_TIME:                      'EARLIEST_TIME';
LATEST:                             'LATEST';
LATEST_TIME:                        'LATEST_TIME';
PER_DAY:                            'PER_DAY';
PER_HOUR:                           'PER_HOUR';
PER_MINUTE:                         'PER_MINUTE';
PER_SECOND:                         'PER_SECOND';
RATE:                               'RATE';
SPARKLINE:                          'SPARKLINE';
C:                                  'C';
DC:                                 'DC';

// BASIC FUNCTIONS
ABS:                                'ABS';
CEIL:                               'CEIL';
CEILING:                            'CEILING';
CONV:                               'CONV';
CRC32:                              'CRC32';
E:                                  'E';
EXP:                                'EXP';
FLOOR:                              'FLOOR';
LN:                                 'LN';
LOG:                                'LOG';
LOG10:                              'LOG10';
LOG2:                               'LOG2';
MOD:                                'MOD';
PI:                                 'PI';
POW:                                'POW';
POWER:                              'POWER';
RAND:                               'RAND';
ROUND:                              'ROUND';
SIGN:                               'SIGN';
SQRT:                               'SQRT';
TRUNCATE:                           'TRUNCATE';

// TRIGONOMETRIC FUNCTIONS
ACOS:                               'ACOS';
ASIN:                               'ASIN';
ATAN:                               'ATAN';
ATAN2:                              'ATAN2';
COS:                                'COS';
COT:                                'COT';
DEGREES:                            'DEGREES';
RADIANS:                            'RADIANS';
SIN:                                'SIN';
TAN:                                'TAN';

// DATE AND TIME FUNCTIONS
ADDDATE:                            'ADDDATE';
DATE:                               'DATE';
DATE_ADD:                           'DATE_ADD';
DATE_SUB:                           'DATE_SUB';
DAYOFMONTH:                         'DAYOFMONTH';
DAYOFWEEK:                          'DAYOFWEEK';
DAYOFYEAR:                          'DAYOFYEAR';
DAYNAME:                            'DAYNAME';
FROM_DAYS:                          'FROM_DAYS';
MAKEDATE:                           'MAKEDATE';
MAKETIME:                           'MAKETIME';
MONTHNAME:                          'MONTHNAME';
SUBDATE:                            'SUBDATE';
TIME:                               'TIME';
TIME_TO_SEC:                        'TIME_TO_SEC';
TIMESTAMP:                          'TIMESTAMP';
DATE_FORMAT:                        'DATE_FORMAT';
TO_DAYS:                            'TO_DAYS';

// TEXT FUNCTIONS
SUBSTR:                             'SUBSTR';
SUBSTRING:                          'SUBSTRING';
LTRIM:                              'LTRIM';
RTRIM:                              'RTRIM';
TRIM:                               'TRIM';
TO:                                 'TO';
LOWER:                              'LOWER';
UPPER:                              'UPPER';
CONCAT:                             'CONCAT';
CONCAT_WS:                          'CONCAT_WS';
LENGTH:                             'LENGTH';
STRCMP:                             'STRCMP';
RIGHT:                              'RIGHT';
LEFT:                               'LEFT';
ASCII:                              'ASCII';
LOCATE:                             'LOCATE';
REPLACE:                            'REPLACE';
CAST:                               'CAST';

// BOOL FUNCTIONS
LIKE:                               'LIKE';
ISNULL:                             'ISNULL';
ISNOTNULL:                          'ISNOTNULL';

// FLOWCONTROL FUNCTIONS
IFNULL:                             'IFNULL';
NULLIF:                             'NULLIF';
IF:                                 'IF';

// RELEVANCE FUNCTIONS AND PARAMETERS
MATCH:                              'MATCH';
MATCH_PHRASE:                       'MATCH_PHRASE';
MATCH_PHRASE_PREFIX:                'MATCH_PHRASE_PREFIX';
MATCH_BOOL_PREFIX:                  'MATCH_BOOL_PREFIX';
SIMPLE_QUERY_STRING:                'SIMPLE_QUERY_STRING';
MULTI_MATCH:                        'MULTI_MATCH';
QUERY_STRING:                       'QUERY_STRING';

ALLOW_LEADING_WILDCARD:             'ALLOW_LEADING_WILDCARD';
ANALYZE_WILDCARD:                   'ANALYZE_WILDCARD';
ANALYZER:                           'ANALYZER';
AUTO_GENERATE_SYNONYMS_PHRASE_QUERY:'AUTO_GENERATE_SYNONYMS_PHRASE_QUERY';
BOOST:                              'BOOST';
CUTOFF_FREQUENCY:                   'CUTOFF_FREQUENCY';
DEFAULT_FIELD:                      'DEFAULT_FIELD';
DEFAULT_OPERATOR:                   'DEFAULT_OPERATOR';
ENABLE_POSITION_INCREMENTS:         'ENABLE_POSITION_INCREMENTS';
ESCAPE:                             'ESCAPE';
FLAGS:                              'FLAGS';
FUZZY_MAX_EXPANSIONS:               'FUZZY_MAX_EXPANSIONS';
FUZZY_PREFIX_LENGTH:                'FUZZY_PREFIX_LENGTH';
FUZZY_TRANSPOSITIONS:               'FUZZY_TRANSPOSITIONS';
FUZZY_REWRITE:                      'FUZZY_REWRITE';
FUZZINESS:                          'FUZZINESS';
LENIENT:                            'LENIENT';
LOW_FREQ_OPERATOR:                  'LOW_FREQ_OPERATOR';
MAX_DETERMINIZED_STATES:            'MAX_DETERMINIZED_STATES';
MAX_EXPANSIONS:                     'MAX_EXPANSIONS';
MINIMUM_SHOULD_MATCH:               'MINIMUM_SHOULD_MATCH';
OPERATOR:                           'OPERATOR';
PHRASE_SLOP:                        'PHRASE_SLOP';
PREFIX_LENGTH:                      'PREFIX_LENGTH';
QUOTE_ANALYZER:                     'QUOTE_ANALYZER';
QUOTE_FIELD_SUFFIX:                 'QUOTE_FIELD_SUFFIX';
REWRITE:                            'REWRITE';
SLOP:                               'SLOP';
TIE_BREAKER:                        'TIE_BREAKER';
TYPE:                               'TYPE';
ZERO_TERMS_QUERY:                   'ZERO_TERMS_QUERY';

// SPAN KEYWORDS
SPAN:                               'SPAN';
MS:                                 'MS';
S:                                  'S';
M:                                  'M';
H:                                  'H';
W:                                  'W';
Q:                                  'Q';
Y:                                  'Y';


// LITERALS AND VALUES
//STRING_LITERAL:                     DQUOTA_STRING | SQUOTA_STRING | BQUOTA_STRING;
ID:                                 ID_LITERAL;
INTEGER_LITERAL:                    DEC_DIGIT+;
DECIMAL_LITERAL:                    (DEC_DIGIT+)? '.' DEC_DIGIT+;

fragment DATE_SUFFIX:               ([\-.][*0-9]+)*;
fragment ID_LITERAL:                [@*A-Z]+?[*A-Z_\-0-9]*;
ID_DATE_SUFFIX:                     ID_LITERAL DATE_SUFFIX;
DQUOTA_STRING:                      '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
SQUOTA_STRING:                      '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';
BQUOTA_STRING:                      '`' ( '\\'. | '``' | ~('`'|'\\'))* '`';
fragment DEC_DIGIT:                 [0-9];


ERROR_RECOGNITION:                  .    -> channel(ERRORCHANNEL);
