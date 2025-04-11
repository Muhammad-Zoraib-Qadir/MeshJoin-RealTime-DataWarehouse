package Connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private static final String URL = "jdbc:mysql://localhost:3306/Metro"; // Database URL
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver"; // JDBC Driver

    public static Connection getConnection(String user, String password) throws SQLException {
        try {
            Class.forName(DRIVER);
            return DriverManager.getConnection(URL, user, password);
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC Driver not found.", e);
        }
    }
}
