 --create stored procedure for PlaceOrder
 CREATE PROCEDURE PlaceOrder
    @CustomerID INT,
    @OrderItems OrderItemType READONLY
AS
BEGIN 
    SET NOCOUNT ON;

    DECLARE @OrderID INT;

    --Insertion into Orders 
    --NB TotalAmount to be updated later
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES( @CustomerID, GETDATE(),0);

    --Get the new OrderID
    SET @OrderID = SCOPE_IDENTITY();

    --Insert into OrderDetails
    INSERT INTO OrderDetails (OrderID, ProductID,Quantity,PriceAtOrder)
    SELECT 
        @OrderID,
        p.ProductID,
        i.Quantity,
        p.Price
        FROM
            @OrderItems i
        JOIN Products p ON p.ProductID = i.ProductID;


    --Update stock and log inventory change
    DECLARE @ProductID INT, @Quantity INT;

    DECLARE item_cursor CURSOR FOR 
    SELECT @ProductID, @Quantity FROM OrderItems;

    OPEN item_cursor;
    FETCH NEXT FROM item_cursor INTO @ProductID, @Quantity;

    WHILE @@FETCH_STATUS = 0
    BEGIN 
        --UPDATE STOCK
        UPDATE Products
        SET StockQuantity = StockQuantity - @Quantity
        WHERE ProductID = @ProductID;


        --LOG CHANGE IN INVENTORY 
        INSERT INTO Inventorylogs(ProductID, QuantityChanged, ChangeType)
        VALUES (@ProductID, -@Quantity, 'Order');

        FETCH NEXT FROM item_cursor INTO @ProductID, @Quantity;

    END
    CLOSE item_cursor;
    DEALLOCATE item_cursor;

    --UPDATE TOTAL AMOUNT IN ORDERS
    UPDATE Orders
    SET TotalAmount = (
        SELECT SUM(Quantity * PriceAtOrder)
        FROM OrderDetails
        WHERE OrderID = @OrderID
    )
    WHERE OrderID = @OrderID;

    PRINT 'Order placed successfully with OrderID:' + CAST(@OrderID as VARCHAR)

END