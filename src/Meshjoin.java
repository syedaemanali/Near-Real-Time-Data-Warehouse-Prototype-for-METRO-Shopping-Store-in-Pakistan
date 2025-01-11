import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class Meshjoin implements Runnable {
    // Database connection details

    private static final int MAX_RETRIES = 5;

    private BlockingQueue<String[]> sharedQueue; // Shared queue with Main
    private Hashtable<String, String[]> hashtable = new Hashtable<>();
    private Queue<String> queue = new LinkedList<>();
    private int bufferID = 1; // Auto-increment buffer ID

    private List<String[]> productBuffer = new ArrayList<>(); // Disk buffer for products
    private List<String[]> customerBuffer = new ArrayList<>(); // Disk buffer for customers
    private Set<String> productUniqueSet = new HashSet<>(); // Set to track unique product records
    private Set<String> customerUniqueSet = new HashSet<>(); // Set to track unique customer records
    private int partitionSize = 50; // Number of records per partition

    private Connection conn;

    // Constructor
    public Meshjoin(BlockingQueue<String[]> sharedQueue) {
        this.sharedQueue = sharedQueue;
        // Prompt user for database credentials
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the database URL: ");
        String dbUrl = scanner.nextLine();

        System.out.print("Enter the username: ");
        String user = scanner.nextLine();

        System.out.print("Enter the password: ");
        String password = scanner.nextLine();
        scanner.close(); // Close scanner after reading inputs

        // Establish database connection
        this.conn = connectToDatabase(dbUrl, user, password);
        if (this.conn == null) {
            System.err.println("Could not establish a database connection. Exiting...");
        } else {
            System.out.println("Database connection established.");
        }

        loadMasterData();
    }

    private void loadMasterData() {
        System.out.println("Loading master data into disk buffers...");

        // Loading partitioned file data
        loadPartitionedFileData("C:/Users/syeda/Downloads/products_data - products_data.csv", productBuffer, productUniqueSet, partitionSize);
        loadPartitionedFileData("C:/Users/syeda/Downloads/customers_data - customers_data.csv", customerBuffer, customerUniqueSet, partitionSize);

        System.out.println("Master data loaded successfully.");

        // Wait for 3 seconds before proceeding
        try {
            Thread.sleep(3000);  // Sleep for 3 seconds (3000 milliseconds)
        } catch (InterruptedException e)
        {
            e.printStackTrace();  // Handle interruption exception
        }

        Insert_Dimensions();
    }


    private void loadPartitionedFileData(String filePath, List<String[]> buffer, Set<String> uniqueSet, int partitionSize) {
        //System.out.println("Loading partitioned file " + filePath);
        //System.out.println("Partition size: " + partitionSize);
        //System.out.println("Buffer size: " + buffer.size());
        //System.out.println("UniqueSet size: " + uniqueSet.size());
        try (Scanner scanner = new Scanner(new File(filePath))) {
            List<String[]> partition = new ArrayList<>();
            int lineNumber = 0; // To track line numbers for debugging

            //System.out.println("Reading file: " + filePath);

            while (scanner.hasNextLine()) {
                lineNumber++;
                String line = scanner.nextLine().trim();

                // System.out.println("Line " + lineNumber + ": " + line);

                if (!line.isEmpty()) {
                    String[] record = line.split(","); // Split CSV records into arrays

                    //System.out.println("Parsed record: " + Arrays.toString(record));

                    if (record.length == 0) {
                        System.err.println("Skipping empty record at line " + lineNumber);
                        continue;
                    }

                    String key = record[0].trim(); // Assuming the first column is a unique identifier

                    if (!uniqueSet.contains(key)) {
                        partition.add(record); // Add to current partition
                        uniqueSet.add(key); // Track uniqueness

                        // System.out.println("Added record with key: " + key);
                    } else {
                        //System.out.println("Duplicate key detected, skipping: " + key);
                    }

                    // When partition size is reached, process and clear the partition
                    if (partition.size() == partitionSize) {
                        buffer.addAll(partition); // Add partition to the main buffer

                        //System.out.println("Loaded a partition with " + partition.size() + " records from " + filePath);

                        partition.clear(); // Clear partition for next batch
                    }
                }
            }

            // Add any remaining records in the last partition
            if (!partition.isEmpty()) {
                buffer.addAll(partition);
                //System.out.println("Loaded final partition with " + partition.size() + " records from " + filePath);
            }

            //System.out.println("Total records loaded into buffer from " + filePath + ": " + buffer.size());
        } catch (FileNotFoundException e) {
            System.err.println("Error loading file: " + filePath + ", " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error while loading file: " + filePath + ", " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Poll for a buffer from the shared queue
                String[] streamBuffer = sharedQueue.take();

                // System.out.println("Processing a new buffer in Meshjoin...");

                String bufferKey = "buffer" + bufferID++;

                addBuffer(bufferKey, streamBuffer);

                performMeshJoin(streamBuffer);

                display();
                CalculateMeasures();

            }
        } catch (InterruptedException e) {
            System.err.println("Meshjoin interrupted!");
        } finally {
            closeDatabaseConnection();
        }
    }

    private void Insert_Dimensions() {
        System.out.println("Inserting dimensions...");

        // Read and process product CSV
        try (BufferedReader productReader = new BufferedReader(new FileReader("C:/Users/syeda/Downloads/products_data - products_data.csv"))) {
            String line;
            boolean isFirstLine = true; // Flag to identify the first line
            while ((line = productReader.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false; // Skip the header line
                    continue;
                }

                String[] productData = line.split(",");
                if (productData.length < 7)
                {
                    System.err.println("Invalid product record: " + line);
                    continue; // Skip invalid records
                }

                // Extract data fields
                String productID = productData[0].trim();
                String productName = productData[1].trim();
                double productPrice = 0.0;
                try {
                    productPrice = Double.parseDouble(productData[2].trim());
                } catch (NumberFormatException e) {
                    System.err.println("Invalid price for product record: " + line);
                    continue; // Skip this record
                }
                String supplierID = productData[3].trim();
                String supplierName = productData[4].trim();
                String storeID = productData[5].trim();
                String storeName = productData[6].trim();

                insertProductIfNotExists(productID, productName, productPrice, supplierID, storeID);
                insertStoreIfNotExists(storeID, storeName);
                insertSupplierIfNotExists(supplierID, supplierName);
            }
        } catch (IOException e) {
            System.err.println("Error reading product CSV file: " + e.getMessage());
        }

        // Read and process customer CSV
        try (BufferedReader customerReader = new BufferedReader(new FileReader("C:/Users/syeda/Downloads/customers_data - customers_data.csv"))) {
            String line;
            boolean isFirstLine = true; // Flag to identify the first line
            while ((line = customerReader.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                // Split the record into components (assume comma-separated)
                String[] customerData = line.split(",");
                if (customerData.length < 3) {
                    System.err.println("Invalid customer record: " + line);
                    continue;
                }

                String customerID = customerData[0].trim();
                String customerName = customerData[1].trim();
                String gender = customerData[2].trim();

                insertCustomerIfNotExists(customerID, customerName, gender);
            }
        } catch (IOException e) {
            System.err.println("Error reading customer CSV file: " + e.getMessage());
        }
    }

    private int parseInteger(String value) throws NumberFormatException
    {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e)
        {
            System.err.println("Invalid integer value: " + value);
            throw e;
        }
    }

    private double parseDouble(String value) throws NumberFormatException {
        try {
            return Double.parseDouble(value.trim());
        }
        catch (NumberFormatException e)
        {
            System.err.println("Invalid double value: " + value);
            throw e;
        }
    }

    private void performMeshJoin(String[] streamBuffer)
    {
        for (String streamRecord : streamBuffer)
        {
            if (streamRecord == null || streamRecord.trim().isEmpty())
            {
                System.err.println("Stream record is null or empty!");
                continue;
            }

            String[] streamData = streamRecord.split(",");
            boolean matched = false;

            // Extract time data
            String orderDate = streamData[1];
            String timeID = streamData[5];
            TimeDimensionData timeData = extractTimeData(orderDate);
            if (timeData == null)
            {
                System.err.println("Skipping record due to invalid time data.");
                continue;
            }

            // Handle TimeDimension insertion
            insertTimeDimensionIfNotExists(timeID, orderDate, timeData);

            // Process Product Buffer
            matched = processProductBuffer(streamData, timeID) || matched;

            // Process Customer Buffer
            matched = processCustomerBuffer(streamData, timeID) || matched;

            if (!matched)
            {
                System.out.println("No match found for stream record: " + String.join(",", streamData));
            }
        }
    }

    // Extract TimeDimensionData
    private TimeDimensionData extractTimeData(String orderDate) {
        try {
            LocalDateTime date = LocalDateTime.parse(orderDate, DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss"));
            int year = date.getYear();
            int month = date.getMonthValue();
            int day = date.getDayOfMonth();
            int week = date.get(WeekFields.ISO.weekOfYear());
            int quarter = (month - 1) / 3 + 1;
            String dayOfWeek = date.getDayOfWeek().toString();
            boolean isWeekend = dayOfWeek.equals("SATURDAY") || dayOfWeek.equals("SUNDAY");
            return new TimeDimensionData(year, month, day, week, quarter, dayOfWeek, isWeekend);
        } catch (DateTimeParseException e) {
            //System.err.println("Error parsing Order_Date: " + e.getMessage());
            return null;
        }
    }

    // Insert into TimeDimension if not exists
    private void insertTimeDimensionIfNotExists(String timeID, String orderDate, TimeDimensionData timeData) {
        if (alreadyExists("TimeDimension", "Time_ID", timeID)) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TimeDimension (Time_ID, Order_Date, Year, Month, Day, Week, Quarter, Day_Of_Week, Is_Weekend) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, timeID);
            ps.setTimestamp(2, Timestamp.valueOf(orderDate));
            ps.setInt(3, timeData.year);
            ps.setInt(4, timeData.month);
            ps.setInt(5, timeData.day);
            ps.setInt(6, timeData.week);
            ps.setInt(7, timeData.quarter);
            ps.setString(8, timeData.dayOfWeek);
            ps.setBoolean(9, timeData.isWeekend);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting TimeDimension: " + e.getMessage());
        }
    }

    // Process Product Buffer
    private boolean processProductBuffer(String[] streamData, String timeID) {
        boolean matched = false;
        for (String[] productRecord : productBuffer)
        {
            // Ensure join condition is met before proceeding
            if (!joinCondition_ProductMD(streamData, productRecord))
            {
                continue;
            }
            matched = true;

            try {
                // Extract values from productRecord
                String productID = productRecord[0];
                String productName = productRecord[1];
                double productPrice = parseDouble(productRecord[2]);
                String supplierID = productRecord[3];
                String supplierName = productRecord[4];
                String storeID = productRecord[5];
                String storeName = productRecord[6];
                String orderID = streamData[0];
                int quantityOrdered = parseInteger(streamData[3]);
                String CustomerID = streamData[4];

                // Insert records only if they do not exist
                insertProductIfNotExists(productID, productName, productPrice, supplierID, storeID);
                insertSupplierIfNotExists(supplierID, supplierName);
                insertStoreIfNotExists(storeID, storeName);

                // Check if the order already exists
                if (!alreadyExists("sales_fact", "Order_ID", orderID)) {
                    // Insert new sales_fact record
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO sales_fact (Product_ID, Order_ID, Supplier_ID, Store_ID, Time_ID, Quantity_Ordered, Customer_ID) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                        // Set values in the prepared statement
                        ps.setString(1, productID);
                        ps.setString(2, orderID);
                        ps.setString(3, supplierID);
                        ps.setString(4, storeID);
                        ps.setString(5, timeID);
                        ps.setInt(6, quantityOrdered);
                        ps.setString(7, CustomerID);

                        // Execute the update
                        ps.executeUpdate();
                    } catch (SQLException e) {
                       // System.err.println("SQL Error processing product record: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing product record: " + e.getMessage());
            }
        }
        return matched;
    }

    private boolean processCustomerBuffer(String[] streamData, String timeID)
    {
        boolean matched = false;

        for (String[] customerRecord : customerBuffer)
        {
            if (!joinCondition_CustomerMD(streamData, customerRecord))
            {
                continue; // Skip if join condition fails
            }
            matched = true;

            // Extract order and product details from streamData before using orderID
            String orderID = streamData[0];
            String productID = streamData[2];
            int quantityOrdered = parseInteger(streamData[3]);

            // Check if order exists in the sales_fact table
            if (!alreadyExists("sales_fact", "Order_ID", orderID)) {
                try
                {
                    // Extract customer details
                    String customerID = customerRecord[0];
                    String customerName = customerRecord[1];
                    String customerGender = customerRecord[2];

                    // Retrieve storeID and supplierID corresponding to the productID
                    String storeID = getStoreIDForProduct(productID);
                    String supplierID = getSupplierIDForProduct(productID);

                    // Make sure the customer exists in the customer table
                    insertCustomerIfNotExists(customerID, customerName, customerGender);

                    // Insert into sales_fact table
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO sales_fact (Customer_ID, Order_ID, Time_ID, Quantity_Ordered, Product_ID, Supplier_ID, Store_ID) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                        // Set values in the prepared statement
                        ps.setString(1, customerID);
                        ps.setString(2, orderID);
                        ps.setString(3, timeID);
                        ps.setInt(4, quantityOrdered);
                        ps.setString(5, productID);
                        ps.setString(6, supplierID);
                        ps.setString(7, storeID);
                        ps.executeUpdate();
                        System.out.println("Record Inserted");
                    }
                } catch (SQLException e) {
                    //System.err.println("SQL Error processing customer record: " + e.getMessage());
                } catch (Exception e) {
                    //System.err.println("General Error processing customer record: " + e.getMessage());
                }
            }
        }
        return matched;
    }

    // Helper method to retrieve storeID based on productID
    private String getStoreIDForProduct(String productID) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT Store_ID FROM products WHERE Product_ID = ?")) {
            ps.setString(1, productID);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("Store_ID");  // Return Store ID as String
                } else {
                    throw new SQLException("No store found for Product_ID: " + productID);
                }
            }
        }
    }

    // Helper method to retrieve supplierID based on productID
    private String getSupplierIDForProduct(String productID) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT Supplier_ID FROM products WHERE Product_ID = ?")) {
            ps.setString(1, productID);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("Supplier_ID");  // Return Supplier ID as String
                } else {
                    throw new SQLException("No supplier found for Product_ID: " + productID);
                }
            }
        }
    }


    // Reusable Insert Methods
    private void insertProductIfNotExists(String productID, String productName, double productPrice, String SupplierID, String StoreID) {
        if (alreadyExists("Products", "Product_ID", productID))
        {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Products (Product_ID, Product_Name, Price,Supplier_ID,Store_ID) VALUES (?, ?, ?,?,?)")) {
            ps.setString(1, productID);
            ps.setString(2, productName);
            ps.setDouble(3, productPrice);
            ps.setString(4, SupplierID);
            ps.setString(5, StoreID);
            ps.executeUpdate();
        } catch (SQLException e)
        {
            System.err.println("Error inserting Product: " + e.getMessage());
        }
    }

    private void insertSupplierIfNotExists(String supplierID, String supplierName) {
        if (supplierID == null || supplierID.trim().isEmpty() || supplierName == null || supplierName.trim().isEmpty()) {
            //System.err.println("Invalid Supplier record. Skipping insertion.");
            return;
        }

        if (alreadyExists("Supplier", "Supplier_ID", supplierID)) {
            return;
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Supplier (Supplier_ID, Supplier_Name) VALUES (?, ?)")) {
            ps.setString(1, supplierID.trim());
            ps.setString(2, supplierName.trim());
            ps.executeUpdate();
        } catch (SQLException e) {
            //System.err.println("Error inserting Supplier: " + e.getMessage());
        }
    }

    private void insertStoreIfNotExists(String storeID, String storeName) {
        // Validate Store ID and Store Name
        if (storeID == null || storeID.trim().isEmpty() || storeName == null || storeName.trim().isEmpty()) {
            System.err.println("Invalid Store record. Skipping insertion.");
            return;
        }

        // Handle commas and quotes in storeName
        storeName = sanitizeQuotedCommasAndTrim(storeName);

        // Checking if the store already exists
        if (alreadyExists("Store", "Store_ID", storeID.trim())) {
            System.out.println("Store already exists. Skipping insertion for ID: " + storeID);
            return;
        }

        // Insert the store into the database
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Store (Store_ID, Store_Name) VALUES (?, ?)")) {
            ps.setString(1, storeID.trim());
            ps.setString(2, storeName);
            ps.executeUpdate();
            System.out.println("Store inserted successfully: " + storeName);
        } catch (SQLException e) {
            System.err.println("Error inserting Store: " + e.getMessage());
        }
    }

    // Helper method to sanitize strings with commas and quotes
    private String sanitizeQuotedCommasAndTrim(String input)
    {
        if (input == null) {
            return "";
        }

        input = input.trim(); // Remove leading and trailing spaces

        // Checking if the string starts and ends with double quotes
        if (input.startsWith("\"") && input.endsWith("\"")) {
            // Remove the enclosing quotes and retain the inner commas/escaped quotes
            input = input.substring(1, input.length() - 1).replace("\"\"", "\"");
        }

        // Replace problematic characters or symbols if necessary
        input = input.replace("\n", " ").replace("\r", " ").trim();

        return input;
    }


    private void insertCustomerIfNotExists(String customerID, String customerName, String customerGender)
    {
        if (alreadyExists("Customers", "Customer_ID", customerID))
        {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Customers (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?)")) {
            ps.setString(1, customerID);
            ps.setString(2, customerName);
            ps.setString(3, customerGender);
            ps.executeUpdate();
        } catch (SQLException e)
        {
            System.err.println("Error inserting Customer: " + e.getMessage());
        }
    }


    // Helper class for TimeDimension data
    private class TimeDimensionData
    {
        int year, month, day, week, quarter;
        String dayOfWeek;
        boolean isWeekend;

        TimeDimensionData(int year, int month, int day, int week, int quarter, String dayOfWeek, boolean isWeekend) {
            this.year = year;
            this.month = month;
            this.day = day;
            this.week = week;
            this.quarter = quarter;
            this.dayOfWeek = dayOfWeek;
            this.isWeekend = isWeekend;
        }
    }

    private boolean alreadyExists(String tableName, String columnName, String idValue)
    {
        String query = "SELECT COUNT(*) FROM " + tableName + " WHERE " + columnName + " = ?";
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, idValue);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            System.err.println("Error checking existence in table " + tableName + ": " + e.getMessage());
        }
        return false;
    }


    private boolean joinCondition_ProductMD(String[] streamData, String[] masterData) {
        if (streamData.length > 4 && masterData.length > 0) {
            String streamProductId = streamData[2].trim();
            String masterProductId = masterData[0].trim();
            System.out.println("StreamProduct ID: " + streamProductId);
            System.out.println("MasterProduct ID: " + masterProductId);

            if (streamProductId.equals(masterProductId)) {
                System.out.println("Matching Product ID found: " + streamProductId);
                return true;
            } else {
                System.out.println("No Matching Product ID found: " + streamProductId);

            }
        }
        return false;

    }


    private boolean joinCondition_CustomerMD(String[] streamData, String[] masterData) {
        if (streamData.length > 0 && masterData.length > 0) {
            String streamCustomerId = streamData[4].trim();
            String masterCustomerId = masterData[0].trim();
            System.out.println("StreamCustomer ID: " + streamCustomerId);
            System.out.println("MasterCustomer ID: " + masterCustomerId);

            if (streamCustomerId.equals(masterCustomerId)) {
                System.out.println("Matching Customer ID found: " + streamCustomerId);
                return true;
            } else {
                System.out.println("No Matching Product ID found: " + streamCustomerId);

            }
        }
        return false;

    }

    public void addBuffer(String key, String[] buffer) {
        queue.add(key); // Add the buffer key to the queue
        hashtable.put(key, buffer); // Add the buffer to the hashtable
        // System.out.println("Added buffer " + key + " with " + buffer.length + " records.");
    }

    public void display()
    {
        System.out.println("Contents of queue and hashtable:");
        System.out.println("Queue: " + queue);
        System.out.println("Hashtable keys: " + hashtable.keySet());
    }

    // Database connection method
    private Connection connectToDatabase(String dbUrl, String user, String password)
    {
        try {
            Connection conn = DriverManager.getConnection(dbUrl, user, password);
            return conn;
        } catch (SQLException e)
        {
            System.err.println("Error connecting to database: " + e.getMessage());
            return null;
        }
    }

    private void closeDatabaseConnection() {
        if (conn != null) {
            try {
                conn.close();
                System.out.println("Database connection closed.");
            } catch (SQLException e) {
                System.err.println("Error closing database connection: " + e.getMessage());
            }
        }
    }

    public void CalculateMeasures() {
        double tot_revenue = 0;
        int total_quantity_sold = 0;

        // Database connection setup (assuming conn is already initialized)
        try {
            String selectQuery = "SELECT s.Sales_ID, s.Quantity_Ordered, p.Price " +
                    "FROM Sales_Fact s " +
                    "JOIN Products p ON s.Product_ID = p.Product_ID";

            PreparedStatement ps = conn.prepareStatement(selectQuery);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                int salesId = rs.getInt("Sales_ID");
                int quantityOrdered = rs.getInt("Quantity_Ordered");
                double price = rs.getDouble("Price");

                // Calculate the revenue for the current sale
                double revenue = quantityOrdered * price;
                revenue = Math.round(revenue * 1000.0) / 1000.0; // Round to 3 decimal points

                tot_revenue += revenue;
                tot_revenue = Math.round(tot_revenue * 1000.0) / 1000.0; // Round cumulative revenue
                total_quantity_sold += quantityOrdered;

                String updateQuery = "UPDATE Sales_Fact " +
                        "SET Total_Units_Sold = ?, Total_Revenue = ? " +
                        "WHERE Sales_ID = ?";

                try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
                    updateStmt.setInt(1, quantityOrdered); // Total Units Sold
                    updateStmt.setDouble(2, revenue);      // Total Revenue
                    updateStmt.setInt(3, salesId);         // Sales ID (to identify the correct row)

                    // Execute the update
                    updateStmt.executeUpdate();
                }
            }

            System.out.println("Total Revenue: " + tot_revenue);
            System.out.println("Total Quantity Sold: " + total_quantity_sold);
            System.out.println("Measures calculated and inserted successfully.");

        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}