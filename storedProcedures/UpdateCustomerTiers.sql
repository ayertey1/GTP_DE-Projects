CREATE PROCEDURE UpdateCustomerTiers
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE Customers
    SET Tier = CASE
        WHEN Total.TotalSpent < 500 THEN 'Bronze'
        WHEN Total.TotalSpent BETWEEN 500 AND 1500 THEN 'Silver'
        WHEN Total.TotalSpent > 1500 THEN 'Gold'
        ELSE 'Bronze'
    END
    FROM Customers C
    JOIN (
        SELECT CustomerID, SUM(TotalAmount) AS TotalSpent
        FROM Orders
        GROUP BY CustomerID
    ) AS Total ON C.CustomerID = Total.CustomerID;

    PRINT 'Customer tiers updated based on total spending.';
END;
