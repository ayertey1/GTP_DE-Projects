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

--customers table
CREATE TABLE Customers(
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100) UNIQUE NOT NULL,
    PhoneNumber NVARCHAR(20)
);

--orders tables 
CREATE TABLE Orders(
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

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

--create Inventorylogs table
CREATE TABLE Inventorylogs (
    LogID INT PRIMARY KEY IDENTITY(1,1),
    ProductID INT NOT NULL,
    ChangeType NVARCHAR(20), -- order or replenish
    QuantityChanged INT,
    ChangeDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

--Create a table type to pass order items 
CREATE TYPE orderItemType AS TABLE (
    ProductID INT,
    Quantity INT
);

--alter table to include categorization 
ALTER TABLE Customers
ADD Tier VARCHAR(10);