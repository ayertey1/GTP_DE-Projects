CREATE VIEW vw_OrderSummary AS
SELECT 
    o.OrderID,
    c.CustomerName,
    o.OrderDate,
    c.Tier,
    o.TotalAmount,
    COUNT(od.ProductID) AS TotalItems
FROM 
    Orders o
JOIN 
    Customers c ON o.CustomerID = c.CustomerID
JOIN 
    OrderDetails od ON o.OrderID = od.OrderID
GROUP BY 
    o.OrderID, c.CustomerName, o.OrderDate, o.TotalAmount, c.Tier;

