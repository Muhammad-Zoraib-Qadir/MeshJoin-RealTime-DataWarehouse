package etl;

import Connection.DatabaseConnection;
import Models.Customer;
import Models.Product;
import Models.Transaction;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
			System.out.println("Enter MySQL Username:");
			String user = scanner.nextLine();

			System.out.println("Enter MySQL Password:");
			String password = scanner.nextLine();

			// Start ETL process in a separate thread
			Thread etlThread = new Thread(() -> {
			    try (Connection connection = DatabaseConnection.getConnection(user, password)) {
			        System.out.println("Connected to the database.");

			        // Load data from CSV files
			        List<Customer> customers = loadCustomers("resources/customers_data.csv");
			        List<Product> products = loadProducts("resources/products_data.csv");
			        List<Transaction> transactions = loadTransactions("resources/transactions.csv");

			        // Perform join (if needed)
			        MeshJoin meshJoin = new MeshJoin(products, transactions);
			        meshJoin.performJoin();

			        // Insert data into tables
			        transformAndLoad(customers, products, transactions, connection);

			        System.out.println("ETL process completed successfully!");

			        // Execute OLAP queries in separate threads

			        // Query 1
			        OLAPQueryExecutor queryExecutor1 = new OLAPQueryExecutor(user, password, "SELECT p.product_name, MONTH(d.date) AS month, " +
			                "CASE WHEN d.is_weekend = TRUE THEN 'Weekend' ELSE 'Weekday' END AS day_type, " +
			                "SUM(s.total_sale) AS total_revenue " +
			                "FROM sales s JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id WHERE d.year = 2019 " +
			                "GROUP BY p.product_name, MONTH(d.date), day_type " +
			                "ORDER BY total_revenue DESC LIMIT 5;");
			        queryExecutor1.start(); // Start first query execution
			        queryExecutor1.join(); // Wait for Query 1 to finish

			        // Query 2
			        OLAPQueryExecutor queryExecutor2 = new OLAPQueryExecutor(user, password, "WITH quarterly_revenue AS (SELECT p.store_id, p.store_name, d.year, d.quarter, " +
			                "SUM(s.total_sale) AS total_revenue FROM sales s " +
			                "JOIN dates d ON s.date_id = d.date_id JOIN products p ON s.product_id = p.product_id " +
			                "WHERE d.year = 2019 GROUP BY p.store_id, p.store_name, d.year, d.quarter), " +
			                "revenue_with_lag AS (SELECT qr.store_id, qr.store_name, qr.quarter, qr.total_revenue, " +
			                "LAG(qr.total_revenue) OVER (PARTITION BY qr.store_id ORDER BY qr.quarter) AS previous_quarter_revenue " +
			                "FROM quarterly_revenue qr) SELECT store_id, store_name, quarter, total_revenue, previous_quarter_revenue, " +
			                "CASE WHEN previous_quarter_revenue IS NOT NULL THEN " +
			                "((total_revenue - previous_quarter_revenue) / previous_quarter_revenue) * 100 ELSE NULL END AS growth_rate " +
			                "FROM revenue_with_lag ORDER BY store_id, quarter;");
			        queryExecutor2.start(); // Start second query execution
			        queryExecutor2.join(); // Wait for Query 1 to finish

			        // Query 3 - Detailed Supplier Sales Contribution by Store and Product Name
			        String query3 = "SELECT p.store_id, p.store_name, p.supplier_id, p.supplier_name, p.product_name, " +
			                "SUM(s.total_sale) AS total_sales_contribution " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "GROUP BY p.store_id, p.store_name, p.supplier_id, p.supplier_name, p.product_name " +
			                "ORDER BY p.store_id, p.store_name, p.supplier_name, p.product_name;";
			        OLAPQueryExecutor queryExecutor3 = new OLAPQueryExecutor(user, password, query3);
			        queryExecutor3.start(); // Start third query execution
			        queryExecutor3.join(); // Wait for Query 1 to finish
			        // Query 4 - Seasonal Sales Analysis with JOIN
			        String query4 = "SELECT " +
			                "p.product_name, " +
			                "CASE " +
			                "WHEN d.month IN (3, 4, 5) THEN 'Spring' " +
			                "WHEN d.month IN (6, 7, 8) THEN 'Summer' " +
			                "WHEN d.month IN (9, 10, 11) THEN 'Fall' " +
			                "WHEN d.month IN (12, 1, 2) THEN 'Winter' " +
			                "ELSE 'Unknown' " +
			                "END AS season, " +
			                "SUM(s.total_sale) AS total_sales " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id " +
			                "WHERE d.year = 2019 " +
			                "GROUP BY p.product_name, season " +
			                "ORDER BY p.product_name, FIELD(season, 'Spring', 'Summer', 'Fall', 'Winter');";
			        OLAPQueryExecutor queryExecutor4 = new OLAPQueryExecutor(user, password, query4);
			        queryExecutor4.start(); // Start fourth query execution
			        queryExecutor4.join(); // Wait for Query 1 to finish
			        // Query 5 - Store-Wise and Supplier-Wise Monthly Revenue Volatility
			        String query5 = "SELECT p.store_name, p.supplier_name, d.month, SUM(s.total_sale) AS monthly_revenue, " +
			                "LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month) AS previous_month_revenue, " +
			                "(SUM(s.total_sale) - LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month)) " +
			                "/ LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month) * 100 AS volatility " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id " +
			                "GROUP BY p.store_name, p.supplier_name, d.month " +
			                "ORDER BY p.store_name, p.supplier_name, d.month;";
			        OLAPQueryExecutor queryExecutor5 = new OLAPQueryExecutor(user, password, query5);
			        queryExecutor5.start(); // Start fifth query execution
			        queryExecutor5.join(); // Wait for Query 1 to finish
			        // Query 6 - Top 5 Products Purchased Together Across Multiple Orders
			        String query6 = "SELECT p1.product_name AS product_1, p2.product_name AS product_2, COUNT(*) AS frequency " +
			                "FROM sales s1 " +
			                "JOIN sales s2 ON s1.order_id = s2.order_id AND s1.product_id != s2.product_id " +
			                "JOIN products p1 ON s1.product_id = p1.product_id " +
			                "JOIN products p2 ON s2.product_id = p2.product_id " +
			                "GROUP BY p1.product_name, p2.product_name " +
			                "ORDER BY frequency DESC " +
			                "LIMIT 5;";
			        OLAPQueryExecutor queryExecutor6 = new OLAPQueryExecutor(user, password, query6);
			        queryExecutor6.start(); // Start sixth query execution
			        queryExecutor6.join(); // Wait for Query 1 to finish
			        // Query 7 - Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
			        String query7 = "SELECT p.store_name, p.supplier_name, p.product_name, SUM(s.total_sale) AS total_sales " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "GROUP BY p.store_name, p.supplier_name, p.product_name WITH ROLLUP " +
			                "ORDER BY p.store_name, p.supplier_name, p.product_name;";
			        OLAPQueryExecutor queryExecutor7 = new OLAPQueryExecutor(user, password, query7);
			        queryExecutor7.start(); // Start seventh query execution
			        queryExecutor7.join(); // Wait for Query 1 to finish
			        // Query 8 - Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
			        String query8 = "SELECT p.product_name, SUM(CASE WHEN MONTH(d.date) <= 6 THEN s.total_sale ELSE 0 END) AS H1_revenue, " +
			                "SUM(CASE WHEN MONTH(d.date) > 6 THEN s.total_sale ELSE 0 END) AS H2_revenue, " +
			                "SUM(CASE WHEN MONTH(d.date) <= 6 THEN s.quantity_ordered ELSE 0 END) AS H1_quantity, " +
			                "SUM(CASE WHEN MONTH(d.date) > 6 THEN s.quantity_ordered ELSE 0 END) AS H2_quantity, " +
			                "SUM(s.total_sale) AS total_revenue, SUM(s.quantity_ordered) AS total_quantity " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id " +
			                "GROUP BY p.product_name " +
			                "ORDER BY p.product_name;";
			        OLAPQueryExecutor queryExecutor8 = new OLAPQueryExecutor(user, password, query8);
			        queryExecutor8.start(); // Start eighth query execution
			        queryExecutor8.join(); // Wait for Query 1 to finish
			        // Query 9 - Identify Outliers in Product Sales Performance
			        String query9 = "WITH daily_sales AS (SELECT p.product_id, p.product_name, d.date, " +
			                "SUM(s.total_sale) AS daily_sales FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id " +
			                "GROUP BY p.product_id, p.product_name, d.date), " +
			                "product_avg_sales AS (SELECT product_id, product_name, AVG(daily_sales) AS daily_avg_sales " +
			                "FROM daily_sales GROUP BY product_id, product_name), " +
			                "spikes AS (SELECT ds.product_id, ds.product_name, ds.date, ds.daily_sales, pas.daily_avg_sales, " +
			                "CASE WHEN ds.daily_sales > 2 * pas.daily_avg_sales THEN 'Outlier' ELSE 'Normal' END AS status " +
			                "FROM daily_sales ds JOIN product_avg_sales pas ON ds.product_id = pas.product_id) " +
			                "SELECT product_id, product_name, date, daily_sales, daily_avg_sales, status " +
			                "FROM spikes " +
			                "ORDER BY product_id, date;";
			        OLAPQueryExecutor queryExecutor9 = new OLAPQueryExecutor(user, password, query9);
			        queryExecutor9.start(); // Start ninth query execution
			        queryExecutor9.join(); // Wait for Query 1 to finish
			        // Query 10 - Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
			        /*
			        String query10 = "CREATE VIEW STORE_QUARTERLY_SALES AS SELECT p.store_name, QUARTER(d.date) AS quarter, " +
			                "SUM(s.total_sale) AS total_sales " +
			                "FROM sales s " +
			                "JOIN products p ON s.product_id = p.product_id " +
			                "JOIN dates d ON s.date_id = d.date_id " +
			                "GROUP BY p.store_name, QUARTER(d.date) " +
			                "ORDER BY p.store_name, quarter;";
			        OLAPQueryExecutor queryExecutor10 = new OLAPQueryExecutor(user, password, query10);
			        queryExecutor10.start(); // Start tenth query execution
			        queryExecutor10.join(); // Wait for Query 1 to finish
			        */
			    } catch (SQLException | IOException e) {
			        System.out.println("Error during ETL or query execution: " + e.getMessage());
			    } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

			// Start the ETL process
			etlThread.start();
		}
    }





    private static List<Customer> loadCustomers(String fileName) throws IOException {
        System.out.println("Loading customers...");
        List<Customer> customers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(","); // Split by comma instead of tab
                if (fields.length == 3) { // Ensure there are exactly 3 fields
                    customers.add(new Customer(Integer.parseInt(fields[0]), fields[1], fields[2]));
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }
        System.out.println("Customers loaded.");
        return customers;
    }


    private static List<Product> loadProducts(String fileName) throws IOException {
        System.out.println("Loading products...");
        List<Product> products = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(","); // Split by comma
                if (fields.length == 7) { // Ensure there are exactly 7 fields
                    try {
                        products.add(new Product(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            Double.parseDouble(fields[2].replace("$", "")), // Remove the dollar sign before parsing as double
                            Integer.parseInt(fields[3]),
                            fields[4],
                            Integer.parseInt(fields[5]),
                            fields[6]
                        ));
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid line: " + line);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }
        System.out.println("Products loaded.");
        return products;
    }


    private static List<Transaction> loadTransactions(String fileName) throws IOException {
        System.out.println("Loading transactions...");
        List<Transaction> transactions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(","); // Split by comma
                if (fields.length == 6) { // Ensure there are exactly 6 fields
                    try {
                        // Convert the string to a Timestamp object
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp orderDate = new Timestamp(sdf.parse(fields[1]).getTime());

                        transactions.add(new Transaction(
                            Integer.parseInt(fields[0]),  // Order ID
                            orderDate,                    // Order Date
                            Integer.parseInt(fields[2]),  // Product ID
                            Integer.parseInt(fields[3]),  // Quantity Ordered
                            Integer.parseInt(fields[4]),  // Customer ID
                            Integer.parseInt(fields[5])   // Time ID
                        ));
                    } catch (Exception e) {
                        System.err.println("Skipping invalid line: " + line);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }
        System.out.println("Transactions loaded.");
        return transactions;
    }

    private static void transformAndLoad(List<Customer> customers, List<Product> products, List<Transaction> transactions, Connection connection) throws Exception {
        try (PreparedStatement customerStmt = connection.prepareStatement("INSERT INTO customers (customer_id, customer_name, gender) VALUES (?, ?, ?)");
             PreparedStatement productStmt = connection.prepareStatement("INSERT INTO products (product_id, product_name, product_price, supplier_id, supplier_name, store_id, store_name) VALUES (?, ?, ?, ?, ?, ?, ?)");
             PreparedStatement dateStmt = connection.prepareStatement("INSERT INTO dates (date, day_of_week, is_weekend, month, year, quarter) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE date_id=date_id", Statement.RETURN_GENERATED_KEYS);
             PreparedStatement timeStmt = connection.prepareStatement("INSERT INTO times (hour, minute, second) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE time_id=time_id", Statement.RETURN_GENERATED_KEYS);
             PreparedStatement salesStmt = connection.prepareStatement("INSERT INTO sales (order_id, date_id, time_id, product_id, customer_id, quantity_ordered, total_sale) VALUES (?, ?, ?, ?, ?, ?, ?)")) {

            // Insert customers
            for (Customer customer : customers) {
                // Check if customer already exists
                if (!doesCustomerExist(customer.getCustomerId(), connection)) {
                    customerStmt.setInt(1, customer.getCustomerId());
                    customerStmt.setString(2, customer.getCustomerName());
                    customerStmt.setString(3, customer.getGender());
                    customerStmt.executeUpdate();
                } else {
                    System.out.println("Customer ID " + customer.getCustomerId() + " already exists, skipping insertion.");
                }
            }

            // Insert products
            for (Product product : products) {
                // Check if product already exists
                if (!doesProductExist(product.getProductId(), connection)) {
                    productStmt.setInt(1, product.getProductId());
                    productStmt.setString(2, product.getProductName());
                    productStmt.setDouble(3, product.getProductPrice());
                    productStmt.setInt(4, product.getSupplierId());
                    productStmt.setString(5, product.getSupplierName());
                    productStmt.setInt(6, product.getStoreId());
                    productStmt.setString(7, product.getStoreName());
                    productStmt.executeUpdate();
                } else {
                    System.out.println("Product ID " + product.getProductId() + " already exists, skipping insertion.");
                }
            }
            // Insert sales transactions
            for (Transaction transaction : transactions) {
                int productId = transaction.getProductId();
                int customerId = transaction.getCustomerId();

                // Check if product exists in products table
                if (!doesProductExist(productId, connection)) {
                    //System.out.println("Skipping transaction with order_id " + transaction.getOrderId() + " due to invalid product_id " + productId);
                    continue; // Skip this transaction
                }

                // Check if customer exists in customers table
                if (!doesCustomerExist(customerId, connection)) {
                    //System.out.println("Skipping transaction with order_id " + transaction.getOrderId() + " due to invalid customer_id " + customerId);
                    continue; // Skip this transaction
                }
                
                if (isOrderIdExists(transaction.getOrderId(), connection)) {
                   // System.out.println("Skipping transaction with order_id " + transaction.getOrderId() + " as it already exists in the sales table.");
                    continue;
                }

                // Parse the order date
                Timestamp orderDate = transaction.getOrderDate();

                // Insert into dates table
                Date sqlDate = new Date(orderDate.getTime()); // Convert Timestamp to java.sql.Date
                dateStmt.setDate(1, sqlDate);
                dateStmt.setString(2, new SimpleDateFormat("EEEE").format(orderDate)); // Day of week
                dateStmt.setBoolean(3, isWeekend(orderDate)); // Weekend check
                dateStmt.setInt(4, orderDate.getMonth() + 1); // Month
                dateStmt.setInt(5, orderDate.getYear() + 1900); // Year
                dateStmt.setInt(6, getQuarter(orderDate)); // Quarter
                dateStmt.executeUpdate();

                // Get generated date_id
                int dateId = getDateIdFromDb(connection, sqlDate); 	

                // Insert into times table
                timeStmt.setInt(1, orderDate.getHours());
                timeStmt.setInt(2, orderDate.getMinutes());
                timeStmt.setInt(3, orderDate.getSeconds());
                timeStmt.executeUpdate();

                // Get generated time_id
                int timeId = getTimeIdFromDb(connection, transaction.getOrderDate());

                // Insert into sales table
                salesStmt.setInt(1, transaction.getOrderId());
                salesStmt.setInt(2, dateId);
                salesStmt.setInt(3, timeId);
                salesStmt.setInt(4, productId);
                salesStmt.setInt(5, customerId);
                salesStmt.setInt(6, transaction.getQuantityOrdered());
                salesStmt.setDouble(7, transaction.getQuantityOrdered() * getProductPrice(productId, products)); // Calculate total sale
                salesStmt.executeUpdate();
            }
        }
    }
    private static boolean isOrderIdExists(int orderId, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT 1 FROM sales WHERE order_id = ? LIMIT 1")) {
            stmt.setInt(1, orderId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next(); // If a record exists, return true
            }
        }
    }
    private static int getDateIdFromDb(Connection connection, Date date) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT date_id FROM dates WHERE date = ?")) {
            stmt.setDate(1, date);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("date_id");
                } else {
                    throw new SQLException("Date not found in database");
                }
            }
        }
    }

    private static int getTimeIdFromDb(Connection connection, Timestamp timestamp) throws SQLException {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT time_id FROM times WHERE hour = ? AND minute = ? AND second = ?")) {
            stmt.setInt(1, localDateTime.getHour());
            stmt.setInt(2, localDateTime.getMinute());
            stmt.setInt(3, localDateTime.getSecond());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("time_id");
                } else {
                    throw new SQLException("Time not found in database");
                }
            }
        }
    }

    private static boolean isWeekend(Timestamp timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp.getTime());
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        return dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY;
    }

    private static int getQuarter(Timestamp timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp.getTime());
        int month = cal.get(Calendar.MONTH) + 1; // Calendar months are 0-based
        return (month - 1) / 3 + 1;
    }


    private static boolean doesProductExist(int productId, Connection connection) throws SQLException {
        String query = "SELECT 1 FROM products WHERE product_id = ? LIMIT 1";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, productId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next(); // Returns true if the product exists
            }
        }
    }

    private static boolean doesCustomerExist(int customerId, Connection connection) throws SQLException {
        String query = "SELECT 1 FROM customers WHERE customer_id = ? LIMIT 1";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, customerId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next(); // Returns true if the customer exists
            }
        }
    }
/*
    private static int getGeneratedKey(PreparedStatement stmt) throws SQLException {
        try (ResultSet rs = stmt.getGeneratedKeys()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            throw new SQLException("Failed to retrieve generated key.");
        }
    }

*/
    private static double getProductPrice(int productId, List<Product> products) {
        for (Product product : products) {
            if (product.getProductId() == productId) {
                return product.getProductPrice();
            }
        }
        return 0.0; // Return 0 if product is not found
    }


}
