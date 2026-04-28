import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.ErrorCode;

public class test_error_format {
    public static void main(String[] args) {
        try {
            IndexNotFoundException cause = new IndexNotFoundException("self.dummy");
            ErrorReport report = ErrorReport.wrap(cause)
                .code(ErrorCode.INDEX_NOT_FOUND)
                .location("while fetching index mappings")
                .context("index_name", "self.dummy")
                .build();

            System.out.println("Details: " + report.getDetails());
            System.out.println("Cause message: " + cause.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}