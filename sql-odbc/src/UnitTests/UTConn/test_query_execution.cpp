// clang-format off
#include "pch.h"
#include "unit_test_helper.h"
#include "opensearch_communication.h"
#include "opensearch_helper.h"
// clang-format on

const std::string valid_host = (use_ssl ? "https://localhost" : "localhost");
const std::string valid_port = "9200";
const std::string valid_user = "admin";
const std::string valid_pw = "admin";
const std::string valid_region = "us-west-3";
const std::string query =
    "SELECT Origin FROM opensearch_dashboards_sample_data_flights LIMIT 5";
const std::string all_columns_flights_query =
    "SELECT * FROM opensearch_dashboards_sample_data_flights LIMIT 5";
const std::string some_columns_flights_query =
    "SELECT Origin, OriginWeather FROM opensearch_dashboards_sample_data_flights LIMIT 5";
const std::string invalid_query = "SELECT";
const int EXECUTION_SUCCESS = 0;
const int EXECUTION_ERROR = -1;
const std::string fetch_size = "0";
const int all_columns_flights_count = 25;
const int some_columns_flights_count = 2;
runtime_options valid_conn_opt_val = {
    {valid_host, valid_port, "1", "0"},
    {"BASIC", valid_user, valid_pw, valid_region},
    {use_ssl, false, "", "", "", ""}};

TEST(TestOpenSearchExecDirect, ValidQuery) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());
    EXPECT_EQ(EXECUTION_SUCCESS,
              OpenSearchExecDirect(&conn, some_columns_flights_query.c_str(), fetch_size.c_str()));
}

TEST(TestOpenSearchExecDirect, MissingQuery) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());
    EXPECT_EQ(EXECUTION_ERROR,
              OpenSearchExecDirect(&conn, NULL, fetch_size.c_str()));
}

TEST(TestOpenSearchExecDirect, MissingConnection) {
    EXPECT_EQ(EXECUTION_ERROR,
              OpenSearchExecDirect(NULL, query.c_str(), fetch_size.c_str()));
}

// Conn::ExecDirect

TEST(TestConnExecDirect, ValidQueryAllColumns) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());

    conn.ExecDirect(all_columns_flights_query.c_str(), fetch_size.c_str());
    OpenSearchResult* result = conn.PopResult();
    EXPECT_EQ("SELECT", result->command_type);
    EXPECT_FALSE(result->result_json.empty());
    EXPECT_EQ(all_columns_flights_count, result->num_fields);
    EXPECT_EQ((size_t)all_columns_flights_count, result->column_info.size());
}

TEST(TestConnExecDirect, ValidQuerySomeColumns) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());

    conn.ExecDirect(some_columns_flights_query.c_str(), fetch_size.c_str());
    OpenSearchResult* result = conn.PopResult();
    EXPECT_EQ("SELECT", result->command_type);
    EXPECT_FALSE(result->result_json.empty());
    EXPECT_EQ(some_columns_flights_count, result->num_fields);
    EXPECT_EQ((size_t)some_columns_flights_count, result->column_info.size());
}

TEST(TestConnExecDirect, InvalidQuery) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());

    conn.ExecDirect(invalid_query.c_str(), fetch_size.c_str());
    OpenSearchResult* result = conn.PopResult();
    EXPECT_EQ(NULL, (void*)result);
}

// Conn::PopResult

TEST(TestConnPopResult, PopEmptyQueue) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());

    OpenSearchResult* result = conn.PopResult();
    EXPECT_EQ(NULL, (void*)result);
}

TEST(TestConnPopResult, PopTwoQueryResults) {
    OpenSearchCommunication conn;
    ASSERT_TRUE(conn.ConnectionOptions(valid_conn_opt_val, false, 0, 0));
    ASSERT_TRUE(conn.ConnectDBStart());

    conn.ExecDirect(some_columns_flights_query.c_str(), fetch_size.c_str());
    conn.ExecDirect(all_columns_flights_query.c_str(), fetch_size.c_str());

    // Pop some_columns
    OpenSearchResult* result = conn.PopResult();
    EXPECT_EQ(some_columns_flights_count, result->num_fields);

    // Pop all_columns
    result = conn.PopResult();
    EXPECT_EQ(all_columns_flights_count, result->num_fields);
}
