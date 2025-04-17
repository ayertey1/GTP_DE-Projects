CREATE VIEW vw_LowStockProducts AS
SELECT 
    ProductName,
    Category,
    StockQuantity,
    ReorderLevel,
    (ReorderLevel - StockQuantity) AS UnitsNeeded
FROM 
    Products
WHERE 
    StockQuantity < ReorderLevel;


