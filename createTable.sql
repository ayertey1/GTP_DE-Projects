-- PHASE ONE : DATABASE DESIGN AND SCHEMA IMPLEMENTATION

-- create Database for the inventory
IF NOT EXISTS(
    select name 
    from sys.databases
    where name = N'InventoryOrderDB'
)
BEGIN 
CREATE DATABASE InventoryOrderDB;
END

USE InventoryOrderDB;
GO
-- create tables in the inventory DB

-- product table 
CREATE TABLE Products(
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50),
    Price DECIMAL(10,2) NOT NULL,
    StockQuantity INT NOT NULL,
    ReorderLevel INT NOT NULL
);
-- Index on ProductName for quick searches
CREATE INDEX idx_Products_ProductName ON Products(ProductName);
-- Index on StockQuantity to optimize low stock queries
CREATE INDEX idx_Products_StockQuantity ON Products(StockQuantity);

--customers table
CREATE TABLE Customers(
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100) UNIQUE NOT NULL,
    PhoneNumber NVARCHAR(20)
);
-- Index on CustomerName for faster lookup/reporting
CREATE INDEX idx_Customers_CustomerName ON Customers(CustomerName);

--orders tables 
CREATE TABLE Orders(
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);
-- Index on CustomerID and OrderDate for reporting
CREATE INDEX idx_Orders_CustomerID_OrderDate ON Orders(CustomerID, OrderDate);

--orderDetails Table 
CREATE TABLE OrderDetails(
    OrderDetailID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    PriceAtOrder DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
-- Index on OrderID for fast joins
CREATE INDEX idx_OrderDetails_OrderID ON OrderDetails(OrderID);
-- Index on ProductID to optimize product-specific aggregations
CREATE INDEX idx_OrderDetails_ProductID ON OrderDetails(ProductID);

--create Inventorylogs table
CREATE TABLE Inventorylogs (
    LogID INT PRIMARY KEY IDENTITY(1,1),
    ProductID INT NOT NULL,
    ChangeType NVARCHAR(20), -- order or replenish
    QuantityChanged INT,
    ChangeDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
-- Index on ProductID and ChangeDate for auditing
CREATE INDEX idx_InventoryLogs_ProductID_ChangeDate ON InventoryLogs(ProductID, ChangeDate);

--Create a table type to pass order items 
CREATE TYPE orderItemType AS TABLE (
    ProductID INT,
    Quantity INT
);

--alter table to include categorization 
ALTER TABLE Customers
ADD Tier VARCHAR(10);

