ALTER PROCEDURE PlaceOrder
    @CustomerID INT,
    @OrderItems OrderItemType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OrderID INT;
    DECLARE @ProductID INT, @Quantity INT;
    DECLARE @TotalQty INT, @TotalAmount DECIMAL(10,2), @FinalAmount DECIMAL(10,2);

    -- Insert Order (TotalAmount set later)
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES (@CustomerID, GETDATE(), 0);

    SET @OrderID = SCOPE_IDENTITY();

    -- Insert Order Details
    INSERT INTO OrderDetails (OrderID, ProductID, Quantity, PriceAtOrder)
    SELECT 
        @OrderID,
        p.ProductID,
        i.Quantity,
        p.Price
    FROM @OrderItems i
    JOIN Products p ON p.ProductID = i.ProductID;

    -- Update stock + inventory log
    DECLARE item_cursor CURSOR FOR SELECT ProductID, Quantity FROM @OrderItems;
    OPEN item_cursor;
    FETCH NEXT FROM item_cursor INTO @ProductID, @Quantity;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        UPDATE Products
        SET StockQuantity = StockQuantity - @Quantity
        WHERE ProductID = @ProductID;

        INSERT INTO InventoryLogs (ProductID, QuantityChanged, ChangeType)
        VALUES (@ProductID, -@Quantity, 'Order');

        FETCH NEXT FROM item_cursor INTO @ProductID, @Quantity;
    END

    CLOSE item_cursor;
    DEALLOCATE item_cursor;

    -- Step 1: Calculate total quantity and total amount  ****new modifications starts here
    SELECT 
        @TotalQty = SUM(Quantity),
        @TotalAmount = SUM(Quantity * PriceAtOrder)
    FROM OrderDetails
    WHERE OrderID = @OrderID;

    -- Step 2: Apply 10% discount if quantity > 10
    SET @FinalAmount = CASE 
        WHEN @TotalQty > 10 THEN @TotalAmount * 0.9
        ELSE @TotalAmount
    END;

    --  Step 3: Update TotalAmount in Orders ****new modifications starts here
    UPDATE Orders
    SET TotalAmount = @FinalAmount
    WHERE OrderID = @OrderID;

    PRINT 'Order placed successfully with OrderID: ' + CAST(@OrderID AS VARCHAR);
    PRINT 'Total Quantity: ' + CAST(@TotalQty AS VARCHAR);
    PRINT 'Discount Applied: ' + CASE WHEN @TotalQty > 10 THEN 'Yes (10%)' ELSE 'No' END;
    PRINT 'Final Amount: ' + CAST(@FinalAmount AS VARCHAR);
END;
