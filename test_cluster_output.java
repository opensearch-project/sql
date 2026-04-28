import java.io.*;

public class TestClusterOutput {
    public static void main(String[] args) {
        try {
            // Create a test program to capture the exact output
            String query = "source=opensearch-sql_test_index_account | eval message='login error' | cluster message | head 5";
            System.out.println("Query: " + query);

            // This would simulate what the test would produce
            System.out.println("Expected to generate explain output...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}