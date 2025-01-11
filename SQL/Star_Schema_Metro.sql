-- Drop schema if it exists
DROP SCHEMA IF EXISTS Metro_Star_Schema;
CREATE SCHEMA Metro_Star_Schema;

USE Metro_Star_Schema;

-- Drop existing tables
DROP TABLE IF EXISTS Sales_Fact;
DROP TABLE IF EXISTS TimeDimension;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Store;
DROP TABLE IF EXISTS Supplier;

-- Create tables
CREATE TABLE Customers (
    Customer_ID VARCHAR(255) PRIMARY KEY,
    Customer_Name VARCHAR(255) NOT NULL,
    Gender VARCHAR(50) NOT NULL
);


CREATE TABLE Store (
    Store_ID VARCHAR(255) PRIMARY KEY,
    Store_Name VARCHAR(255) NOT NULL
);

CREATE TABLE Supplier (
    Supplier_ID VARCHAR(255) PRIMARY KEY,
    Supplier_Name VARCHAR(255) NOT NULL
);

CREATE TABLE Products (
    Product_ID VARCHAR(255) PRIMARY KEY,
    Product_Name VARCHAR(255) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    Store_ID VARCHAR(255),
    Supplier_ID VARCHAR(255),
    FOREIGN KEY(Store_ID) REFERENCES Store(Store_ID),
    FOREIGN KEY(Supplier_ID) REFERENCES Supplier(Supplier_ID)
);


-- Updated TimeDimension Table
CREATE TABLE TimeDimension (
    Time_ID VARCHAR(255) NOT NULL,
    Order_Date TIMESTAMP NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    Week INT NOT NULL,
    Quarter INT NOT NULL,
    Day_Of_Week VARCHAR(20),
    Is_Weekend BOOLEAN NOT NULL,
    UNIQUE (Time_ID, Order_Date) -- Composite unique key
);
CREATE TABLE Sales_Fact (
    Sales_ID INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(255) NOT NULL,
    Customer_ID VARCHAR(255) NOT NULL,
    Store_ID VARCHAR(255) NOT NULL,
    Supplier_ID VARCHAR(255) NOT NULL,
    Time_ID VARCHAR(255) NOT NULL,
    Order_ID VARCHAR(255) NOT NULL,
    Quantity_Ordered INT NOT NULL,
    Total_Units_Sold INT DEFAULT 0,                -- Default value of 0
    Total_Revenue DECIMAL(10, 2) DEFAULT 0.00,     -- Default value of 0.00
    FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID),
    FOREIGN KEY (Store_ID) REFERENCES Store(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier(Supplier_ID),
    FOREIGN KEY (Time_ID) REFERENCES TimeDimension(Time_ID)
);




-- Add indexes for performance optimization
CREATE INDEX idx_product_id ON Sales_Fact (Product_ID);
CREATE INDEX idx_customer_id ON Sales_Fact (Customer_ID);
CREATE INDEX idx_store_id ON Sales_Fact (Store_ID);
CREATE INDEX idx_supplier_id ON Sales_Fact (Supplier_ID);
CREATE INDEX idx_time_id ON Sales_Fact (Time_ID);

CREATE INDEX idx_order_date ON TimeDimension (Order_Date);
