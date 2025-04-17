CREATE VIEW vw_CustomerInsights AS
SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    c.PhoneNumber,
    c.Tier,
    COUNT(o.OrderID) AS TotalOrders,
    ISNULL(SUM(o.TotalAmount), 0) AS TotalSpent
FROM 
    Customers c
LEFT JOIN 
    Orders o ON c.CustomerID = o.CustomerID
GROUP BY 
    c.CustomerID, c.CustomerName, c.Email, c.PhoneNumber, c.Tier;
