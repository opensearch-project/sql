/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

/** Created by omershelef on 18/12/14. */
public class TestsConstants {

  public static final String PERSISTENT = "persistent";
  public static final String TRANSIENT = "transient";

  public static final String TEST_INDEX = "opensearch-sql_test_index";

  public static final String TEST_INDEX_ONLINE = TEST_INDEX + "_online";
  public static final String TEST_INDEX_ACCOUNT = TEST_INDEX + "_account";
  public static final String TEST_INDEX_PHRASE = TEST_INDEX + "_phrase";
  public static final String TEST_INDEX_DOG = TEST_INDEX + "_dog";
  public static final String TEST_INDEX_DOG2 = TEST_INDEX + "_dog2";
  public static final String TEST_INDEX_DOG3 = TEST_INDEX + "_dog3";
  public static final String TEST_INDEX_DOGSUBQUERY = TEST_INDEX + "_subquery";
  public static final String TEST_INDEX_PEOPLE = TEST_INDEX + "_people";
  public static final String TEST_INDEX_PEOPLE2 = TEST_INDEX + "_people2";
  public static final String TEST_INDEX_GAME_OF_THRONES = TEST_INDEX + "_game_of_thrones";
  public static final String TEST_INDEX_SYSTEM = TEST_INDEX + "_system";
  public static final String TEST_INDEX_ODBC = TEST_INDEX + "_odbc";
  public static final String TEST_INDEX_LOCATION = TEST_INDEX + "_location";
  public static final String TEST_INDEX_LOCATION2 = TEST_INDEX + "_location2";
  public static final String TEST_INDEX_NESTED_TYPE = TEST_INDEX + "_nested_type";
  public static final String TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS =
      TEST_INDEX + "_nested_type_without_arrays";
  public static final String TEST_INDEX_NESTED_SIMPLE = TEST_INDEX + "_nested_simple";
  public static final String TEST_INDEX_NESTED_WITH_QUOTES =
      TEST_INDEX + "_nested_type_with_quotes";
  public static final String TEST_INDEX_EMPLOYEE_NESTED = TEST_INDEX + "_employee_nested";
  public static final String TEST_INDEX_JOIN_TYPE = TEST_INDEX + "_join_type";
  public static final String TEST_INDEX_UNEXPANDED_OBJECT = TEST_INDEX + "_unexpanded_object";
  public static final String TEST_INDEX_BANK = TEST_INDEX + "_bank";
  public static final String TEST_INDEX_BANK_TWO = TEST_INDEX_BANK + "_two";
  public static final String TEST_INDEX_BANK_WITH_NULL_VALUES =
      TEST_INDEX_BANK + "_with_null_values";
  public static final String TEST_INDEX_BANK_CSV_SANITIZE = TEST_INDEX_BANK + "_csv_sanitize";
  public static final String TEST_INDEX_BANK_RAW_SANITIZE = TEST_INDEX_BANK + "_raw_sanitize";
  public static final String TEST_INDEX_ORDER = TEST_INDEX + "_order";
  public static final String TEST_INDEX_WEBLOGS = TEST_INDEX + "_weblogs";
  public static final String TEST_INDEX_DATE = TEST_INDEX + "_date";
  public static final String TEST_INDEX_DATE_TIME = TEST_INDEX + "_datetime";
  public static final String TEST_INDEX_DEEP_NESTED = TEST_INDEX + "_deep_nested";
  public static final String TEST_INDEX_STRINGS = TEST_INDEX + "_strings";
  public static final String TEST_INDEX_DATATYPE_NUMERIC = TEST_INDEX + "_datatypes_numeric";
  public static final String TEST_INDEX_DATATYPE_NONNUMERIC = TEST_INDEX + "_datatypes_nonnumeric";
  public static final String TEST_INDEX_BEER = TEST_INDEX + "_beer";
  public static final String TEST_INDEX_NULL_MISSING = TEST_INDEX + "_null_missing";
  public static final String TEST_INDEX_CALCS = TEST_INDEX + "_calcs";
  public static final String TEST_INDEX_DATE_FORMATS = TEST_INDEX + "_date_formats";
  public static final String TEST_INDEX_DATE_FORMATS_WITH_NULL = TEST_INDEX + "_date_formats_with_null";
  public static final String TEST_INDEX_WILDCARD = TEST_INDEX + "_wildcard";
  public static final String TEST_INDEX_MULTI_NESTED_TYPE = TEST_INDEX + "_multi_nested";
  public static final String TEST_INDEX_NESTED_WITH_NULLS = TEST_INDEX + "_nested_with_nulls";
  public static final String TEST_INDEX_GEOPOINT = TEST_INDEX + "_geopoint";
  public static final String TEST_INDEX_JSON_TEST = TEST_INDEX + "_json_test";
  public static final String TEST_INDEX_ALIAS = TEST_INDEX + "_alias";
  public static final String TEST_INDEX_GEOIP = TEST_INDEX + "_geoip";
  public static final String DATASOURCES = ".ql-datasources";
  public static final String TEST_INDEX_STATE_COUNTRY = TEST_INDEX + "_state_country";
  public static final String TEST_INDEX_STATE_COUNTRY_WITH_NULL =
      TEST_INDEX + "_state_country_with_null";
  public static final String TEST_INDEX_OCCUPATION = TEST_INDEX + "_occupation";
  public static final String TEST_INDEX_OCCUPATION_TOP_RARE = TEST_INDEX + "_occupation_top_rare";
  public static final String TEST_INDEX_HOBBIES = TEST_INDEX + "_hobbies";
  public static final String TEST_INDEX_WORKER = TEST_INDEX + "_worker";
  public static final String TEST_INDEX_WORK_INFORMATION = TEST_INDEX + "_work_information";
  public static final String TEST_INDEX_DUPLICATION_NULLABLE = TEST_INDEX + "_duplication_nullable";

  public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String TS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String SIMPLE_DATE_FORMAT = "yyyy-MM-dd";

  public static final String SYNTAX_EX_MSG_FRAGMENT =
      "is not a valid term at this part of the query";
}
