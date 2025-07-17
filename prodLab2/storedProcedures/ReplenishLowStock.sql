CREATE PROCEDURE ReplenishLowStock
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ProductID INT, @CurrentQty INT, @ReorderLevel INT, @ReplenishQty INT;

    -- Cursor to loop through low stock products
    DECLARE stock_cursor CURSOR FOR
    SELECT ProductID, StockQuantity, ReorderLevel
    FROM Products
    WHERE StockQuantity < ReorderLevel;

    OPEN stock_cursor;
    FETCH NEXT FROM stock_cursor INTO @ProductID, @CurrentQty, @ReorderLevel;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Define how much to replenish (optional logic — can vary)
        SET @ReplenishQty = (@ReorderLevel + 20) - @CurrentQty;

        -- Update stock
        UPDATE Products
        SET StockQuantity = StockQuantity + @ReplenishQty
        WHERE ProductID = @ProductID;

        -- Log the replenishment
        INSERT INTO InventoryLogs (ProductID, QuantityChanged, ChangeType)
        VALUES (@ProductID, @ReplenishQty, 'Replenishment');

        PRINT 'Replenished ProductID ' + CAST(@ProductID AS VARCHAR) + ' by ' + CAST(@ReplenishQty AS VARCHAR);

        FETCH NEXT FROM stock_cursor INTO @ProductID, @CurrentQty, @ReorderLevel;
    END

    CLOSE stock_cursor;
    DEALLOCATE stock_cursor;

    PRINT 'Stock replenishment completed.';
END;
