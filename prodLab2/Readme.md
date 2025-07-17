# Inventory and Order Management System

## Project Overview

This project implements a full-featured **Inventory and Order Management System** designed for an e-commerce business. The system tracks products, customers, orders, and inventory changes, and supports critical business operations like order placement, automated stock replenishment, customer analysis, and reporting.

## Goals

- Track product inventory, customer data, and order processing efficiently.
- Automatically update stock levels and log inventory changes.
- Provide business insights via reports and customer purchase analysis.
- Automate key processes to reduce manual effort and errors.
- Prepare a system ready for scaling and integration into web applications.

---

## File Structure

```bash
LAB 2/
│
├── README.md                    # Project documentation              
│
├── storedProcedures/            # This directory for stored procedures and procedure call
│   ├── placeOrder.sql           # places order and does computations on total amount
│   ├── procedureCalls.sql       # calls the procedures to be used
│   ├── ReplenishLowStock.sql    # to refill shorts stocks
│   └── UpdateCustomerTiers.sql  # updates the tiers of customers gold/silver/bronze
│               
│
├── views/
|    ├── vw_LowStockProducts.sql    # view for showing stock shorts               
|    └── vw_OrderSummary.sql        # view summarizing Order and customer and their tiers
│               
│
├── createTable.sql   # Database and Tables Creation Script
|
│               
│
├── dataInsertion.sql  # Script for Inserting data
```

---

## Key Features & Deliverables

### 1. **Database Schema**
- Customers, Products, Orders, OrderDetails, InventoryLogs
- Referential integrity using foreign keys
- Reorder logic using `ReorderLevel`

### 2. **Stored Procedures**
- `PlaceOrder`:  
  ‣ Accepts multiple products in a single order  
  ‣ Updates stock quantities  
  ‣ Calculates total and applies bulk discount  
  ‣ Logs inventory changes  
- `ReplenishLowStock`:  
  ‣ Replenishes all products below reorder threshold  
  ‣ Logs replenishments in the inventory log
- `UpdateCustomerTiers`:  
  ‣ Updates the customers tiers based on spending
- `procedureCalls`:  
  ‣ calling the procedures for possible testcase


### 3. **Views**
- `vw_OrderSummary`: Full customer & order item breakdown
- `vw_LowStockProducts`: Real-time list of items needing restocking

### 4. **Reporting Queries**
- Order totals by customer
```
SELECT 
    c.Name,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(o.TotalAmount) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Name;
```
- Product performance (units sold, revenue)
```
SELECT 
    p.ProductName,
    SUM(od.Quantity) AS UnitsSold,
    SUM(od.Quantity * od.PriceAtOrder) AS Revenue
FROM OrderDetails od
JOIN Products p ON od.ProductID = p.ProductID
GROUP BY p.ProductName;
```
- Customer tier classification (Gold/Silver/Bronze)
```
SELECT
    CustomerName,
    Tier
FROM Customers;
```
---

### 5. **Automation Strategy**
- SQL Server Express doesn't support Agent Jobs  
- Stored Procedures were created for Replenish, Customer tier and Placing Orders

---

## Testing

Includes:
- Dummy data for customers, products, and orders (20–50 rows each)
- Manual test cases for:
  - Order placement
  - Stock depletion
  - Auto-replenishment
  - Inventory logging
  - Customer tier changes

---

## Learning Outcomes

✔ Designing normalized databases  
✔ Using stored procedures for logic control    
✔ Applying business logic inside SQL  
✔ Building scalable reporting layers via views

---

## Setup Instructions

1. Clone or download the repository.
2. Run `createTable.sql` to set up the database schema.
3. Run `dataInsertion.sql` to populate dummy data.
4. Run `placeOrder.sql`, `ReplenishLowStock.sql`, and `UpdateCustomerTiers.sql`.
5. Run `procedureCalls.sql` the products and stocks values can be changed in there as desire.
5. Use queries provided in the readme.md to validate system behavior.

---

## Requirements

- SQL Server (Express or Full Edition)
- SQL Server Management Studio (SSMS) or compatible tool

---


