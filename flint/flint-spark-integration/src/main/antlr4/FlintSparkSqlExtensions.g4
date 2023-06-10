/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar FlintSparkSqlExtensions;


// Flint SQL Syntax Extension

singleStatement
    : statement EOF
    ;

statement
    : skippingIndexStatement
    ;

skippingIndexStatement
    : dropSkippingIndexStatement
    ;

dropSkippingIndexStatement
    : DROP SKIPPING INDEX ON multipartIdentifier
    ;


// Flint Lexer Extension

DROP: 'DROP';
INDEX: 'INDEX';
ON: 'ON';
SKIPPING : 'SKIPPING';


// Copy from Spark 3.3.1 SqlBaseParser.g4 and SqlBaseLexer.g4

multipartIdentifier
    : parts+=errorCapturingIdentifier (DOT parts+=errorCapturingIdentifier)*
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
    |                        #realIdent
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

DOT: '.';
MINUS: '-';

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;
