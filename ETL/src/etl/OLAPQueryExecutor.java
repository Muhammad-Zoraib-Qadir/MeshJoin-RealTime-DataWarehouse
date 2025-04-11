package etl;
import java.sql.*;

import Connection.DatabaseConnection;

public class OLAPQueryExecutor extends Thread {
    private final Connection connection;
    private final String query;

    // Constructor that accepts an existing database connection and the query to execute
    public OLAPQueryExecutor(Connection connection, String query) {
        this.connection = connection;
        this.query = query;
    }

    // Constructor that initializes the connection from user credentials and a query
    public OLAPQueryExecutor(String user, String password, String query) throws SQLException {
        this.query = query;
        // Initialize the connection here
        this.connection = DatabaseConnection.getConnection(user, password);
    }

    @Override
    public void run() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("Executing query on thread: " + Thread.currentThread().getName());
            System.out.println("Query:\n" + query);

            ResultSet resultSet = statement.executeQuery(query);

            // Display the column names
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rsMetaData.getColumnLabel(i) + "\t");
            }
            System.out.println("\n-----------------------------------------------");

            // Display the query results
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + "\t");
                }
                System.out.println();
            }

            System.out.println("Query execution completed on thread: " + Thread.currentThread().getName());
        } catch (Exception e) {
            System.err.println("Error executing query on thread: " + Thread.currentThread().getName());
            e.printStackTrace();
        }
    }
}
