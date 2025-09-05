package org.opensearch.sql.spark.asyncquery.indexquery;

import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec;
import org.opensearch.sql.spark.flint.FlintIndexType;

public class IndexQuerySpecBase extends AsyncQueryExecutorServiceSpec {
    private final String specialName = "`test ,:\"+/\\|?#><`";
    private final String encodedName = "test%20%2c%3a%22%2b%2f%5c%7c%3f%23%3e%3c";

    public final String REFRESH_SI = "REFRESH SKIPPING INDEX on mys3.default.http_logs";
    public final String REFRESH_CI = "REFRESH INDEX covering ON mys3.default.http_logs";
    public final String REFRESH_MV = "REFRESH MATERIALIZED VIEW mys3.default.http_logs_metrics";
    public final String REFRESH_SCI = "REFRESH SKIPPING INDEX on mys3.default." + specialName;

    public final FlintDatasetMock LEGACY_SKIPPING =
            new FlintDatasetMock(
                    "DROP SKIPPING INDEX ON mys3.default.http_logs",
                    REFRESH_SI,
                    FlintIndexType.SKIPPING,
                    "flint_mys3_default_http_logs_skipping_index")
                    .isLegacy(true);
    public final FlintDatasetMock LEGACY_COVERING =
            new FlintDatasetMock(
                    "DROP INDEX covering ON mys3.default.http_logs",
                    REFRESH_CI,
                    FlintIndexType.COVERING,
                    "flint_mys3_default_http_logs_covering_index")
                    .isLegacy(true);
    public final FlintDatasetMock LEGACY_MV =
            new FlintDatasetMock(
                    "DROP MATERIALIZED VIEW mys3.default.http_logs_metrics",
                    REFRESH_MV,
                    FlintIndexType.MATERIALIZED_VIEW,
                    "flint_mys3_default_http_logs_metrics")
                    .isLegacy(true);

    public final FlintDatasetMock LEGACY_SPECIAL_CHARACTERS =
            new FlintDatasetMock(
                    "DROP SKIPPING INDEX ON mys3.default." + specialName,
                    REFRESH_SCI,
                    FlintIndexType.SKIPPING,
                    "flint_mys3_default_" + encodedName + "_skipping_index")
                    .isLegacy(true)
                    .isSpecialCharacter(true);

    public final FlintDatasetMock SKIPPING =
            new FlintDatasetMock(
                    "DROP SKIPPING INDEX ON mys3.default.http_logs",
                    REFRESH_SI,
                    FlintIndexType.SKIPPING,
                    "flint_mys3_default_http_logs_skipping_index")
                    .latestId("ZmxpbnRfbXlzM19kZWZhdWx0X2h0dHBfbG9nc19za2lwcGluZ19pbmRleA==");
    public final FlintDatasetMock COVERING =
            new FlintDatasetMock(
                    "DROP INDEX covering ON mys3.default.http_logs",
                    REFRESH_CI,
                    FlintIndexType.COVERING,
                    "flint_mys3_default_http_logs_covering_index")
                    .latestId("ZmxpbnRfbXlzM19kZWZhdWx0X2h0dHBfbG9nc19jb3ZlcmluZ19pbmRleA==");
    public final FlintDatasetMock MV =
            new FlintDatasetMock(
                    "DROP MATERIALIZED VIEW mys3.default.http_logs_metrics",
                    REFRESH_MV,
                    FlintIndexType.MATERIALIZED_VIEW,
                    "flint_mys3_default_http_logs_metrics")
                    .latestId("ZmxpbnRfbXlzM19kZWZhdWx0X2h0dHBfbG9nc19tZXRyaWNz");
    public final FlintDatasetMock SPECIAL_CHARACTERS =
            new FlintDatasetMock(
                    "DROP SKIPPING INDEX ON mys3.default." + specialName,
                    REFRESH_SCI,
                    FlintIndexType.SKIPPING,
                    "flint_mys3_default_" + encodedName + "_skipping_index")
                    .isSpecialCharacter(true)
                    .latestId(
                            "ZmxpbnRfbXlzM19kZWZhdWx0X3Rlc3QlMjAlMmMlM2ElMjIlMmIlMmYlNWMlN2MlM2YlMjMlM2UlM2Nfc2tpcHBpbmdfaW5kZXg=");

    public final String CREATE_SI_AUTO =
            "CREATE SKIPPING INDEX ON mys3.default.http_logs"
                    + "(l_orderkey VALUE_SET) WITH (auto_refresh = true)";

    public final String CREATE_CI_AUTO =
            "CREATE INDEX covering ON mys3.default.http_logs "
                    + "(l_orderkey, l_quantity) WITH (auto_refresh = true)";

    public final String CREATE_MV_AUTO =
            "CREATE MATERIALIZED VIEW mys3.default.http_logs_metrics AS select * "
                    + "from mys3.default.https WITH (auto_refresh = true)";
}
