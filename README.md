# **Near-Real-Time Data Warehouse for METRO Shopping Store**

This project focuses on building a **real-time Data Warehouse (DW)** for METRO Shopping Store in Pakistan. It utilizes the **MESHJOIN algorithm** to process data quickly and help analyze customer behavior, enabling better business decisions.

## **Features**

- **Star Schema**: A simple structure to organize data about sales, customers, products, stores, and suppliers.
- **ETL Process**: Moves data from raw sources into the Data Warehouse in real time.
- **Fast Processing**: Uses multi-threading to process data quickly and efficiently.
- **Business Insights**: Includes useful queries to analyze trends, sales, and customer behavior.


## **Files in the Project**

- **`Star_Schema_Metro.sql`**: A script to set up the Data Warehouse structure.
- **`MeshJoin.java`**: Code for processing and loading data.
- **`OLAP-Queries.sql`**: Queries for analyzing sales trends and customer data.


## **How to Use**

### **Requirements**
- **Java** (JDK 8+)
- **IntelliJ IDEA IDE**
- **MySQL**

### **Steps**
1. Run **`Create-DW.sql`** to set up the database.
2. Add your database details in **`MeshJoin.java`**.
3. Open and run the Java project in **Eclipse**.
4. Use the queries in **`OLAP-Queries.sql`** to analyze the data.

