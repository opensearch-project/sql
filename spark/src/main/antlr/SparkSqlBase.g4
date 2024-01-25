/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications, which are licensed as above:
 */

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

grammar SparkSqlBase;

// Copy from Spark 3.3.1 SqlBaseParser.g4 and SqlBaseLexer.g4

@members {
  /**
   * When true, parser should throw ParseExcetion for unclosed bracketed comment.
   */
  public boolean has_unclosed_bracketed_comment = false;

  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  /**
   * This method will be called when we see '/*' and try to match it as a bracketed comment.
   * If the next character is '+', it should be parsed as hint later, and we cannot match
   * it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method will be called when the character stream ends and try to find out the
   * unclosed bracketed comment.
   * If the method be called, it means the end of the entire character stream match,
   * and we set the flag and fail later.
   */
  public void markUnclosedComment() {
    has_unclosed_bracketed_comment = true;
  }
}


multipartIdentifierPropertyList
    : multipartIdentifierProperty (COMMA multipartIdentifierProperty)*
    ;

multipartIdentifierProperty
    : multipartIdentifier (options=propertyList)?
    ;

propertyList
    : property (COMMA property)*
    ;

property
    : key=propertyKey (EQ? value=propertyValue)?
    ;

propertyKey
    : identifier (DOT identifier)*
    | STRING
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | STRING
    ;

booleanValue
    : TRUE | FALSE
    ;


multipartIdentifier
    : parts+=identifier (DOT parts+=identifier)*
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : DROP | SKIPPING | INDEX
    ;


// Flint lexical tokens

MIN_MAX: 'MIN_MAX';
SKIPPING: 'SKIPPING';
VALUE_SET: 'VALUE_SET';


// Spark lexical tokens

SEMICOLON: ';';

LEFT_PAREN: '(';
RIGHT_PAREN: ')';
COMMA: ',';
DOT: '.';


AS: 'AS';
CREATE: 'CREATE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DROP: 'DROP';
EXISTS: 'EXISTS';
FALSE: 'FALSE';
IF: 'IF';
IN: 'IN';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
JOB: 'JOB';
MATERIALIZED: 'MATERIALIZED';
NOT: 'NOT';
ON: 'ON';
PARTITION: 'PARTITION';
RECOVER: 'RECOVER';
REFRESH: 'REFRESH';
SHOW: 'SHOW';
TRUE: 'TRUE';
VACUUM: 'VACUUM';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
WHERE: 'WHERE';
WITH: 'WITH';


EQ  : '=' | '==';
MINUS: '-';


STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? ( BRACKETED_COMMENT | . )*? ('*/' | {markUnclosedComment();} EOF) -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;