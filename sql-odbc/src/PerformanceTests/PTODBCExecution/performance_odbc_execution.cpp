// clang-format off
#include "chrono"
#include <opensearch_odbc.h>
#include <string>
#include <vector>
#include <iostream>
// clang-format on
#define IT_SIZEOF(x) (NULL == (x) ? 0 : (sizeof((x)) / sizeof((x)[0])))
#define ITERATION_COUNT 12
std::wstring dsn_name = L"DSN=test_dsn";
const wchar_t* const query = L"SELECT * FROM opensearch_dashboards_sample_data_flights limit 10000";

int Setup(SQLHENV* env, SQLHDBC* conn, SQLHSTMT* hstmt) {
    SQLTCHAR out_conn_string[1024];
    SQLSMALLINT out_conn_string_length;
    if (SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env))
        && SQL_SUCCEEDED(SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0))
        && SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, *env, conn))
        && SQL_SUCCEEDED(SQLDriverConnect(*conn, NULL, (SQLTCHAR*)dsn_name.c_str(), SQL_NTS,
                             out_conn_string, IT_SIZEOF(out_conn_string),
                             &out_conn_string_length, SQL_DRIVER_COMPLETE))
        && SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, *conn, hstmt))) {
        return SQL_SUCCESS;
    }
    return SQL_ERROR;
}

void Teardown(SQLHDBC* conn, SQLHENV* env) {
    SQLDisconnect(*conn);
    SQLFreeHandle(SQL_HANDLE_ENV, *env);
}

int QueryExecutionTime() {
    SQLRETURN ret = SQL_ERROR;
    try {
        SQLHENV env = SQL_NULL_HENV;
        SQLHDBC conn = SQL_NULL_HDBC;
        SQLHSTMT hstmt = SQL_NULL_HSTMT;
        if (SQL_SUCCEEDED(Setup(&env, &conn, &hstmt))) {
            std::cout << "Time(ms) for query execution:" << std::endl;
            for (int i = 0; i < ITERATION_COUNT; i++) {
                // Calculate time(ms) for query execution
                auto start = std::chrono::steady_clock::now();
                ret = SQLExecDirect(hstmt, (SQLTCHAR*)query, SQL_NTS);
                auto end = std::chrono::steady_clock::now();
                std::cout<< std::chrono::duration_cast< std::chrono::milliseconds >(
                           end - start).count()<< std::endl;
                SQLCloseCursor(hstmt);
            }
            Teardown(&conn, &env);
        }
    } catch (...) {
        std::cout << "Exception occurred" << std::endl;
    }
    return ret;
}

int main() {
    return QueryExecutionTime();
}
